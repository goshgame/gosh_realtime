package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.job.UserFeatureCommon.PostViewEvent;
import com.gosh.job.UserFeatureCommon.PostViewInfo;
import com.gosh.job.UserFeatureCommon.ViewEventParser;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class UserPornLabelJobV3 {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJobV3.class);

    // Redis Key 前缀和后缀
    private static final String REDIS_KEY_POS = "rec_post:{%d}:rtylevel_v3";               // key1: 正反馈最高等级
    private static final String REDIS_KEY_NEG = "rec_post:{%d}:rtylevel_v3_only_for_degree"; // key2: 负反馈最低等级

    private static final String CleanTag = "clean";
    private static final int REDIS_TTL_POS = 3 * 3600;     // key1: 3小时
    private static final int REDIS_TTL_NEG = 15 * 60;      // key2: 15分钟

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "rec_porn_label_v3";

    // 需要详细日志监控的用户 UID 列表
    private static final Set<Long> MONITORED_UIDS = new HashSet<>(Arrays.asList(
            13659161L,  // 新用户
            13120233L,  // 老用户 / 专区用户
            13374748L,  // 非专区用户
            13661418L,   // 非专区用户（重度色情）
            13687026L,
            13372756L
    ));

    /**
     * 判断是否需要为指定 UID 输出详细日志
     */
    private static boolean shouldLogDetail(long uid) {
        return MONITORED_UIDS.contains(uid);
    }

    public static void main(String[] args) throws Exception {
        try {
            // 1. 创建 Flink 环境
            StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

            // 2. 创建 Kafka Source
            Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);
            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post"
            );

            // 3. 使用 KafkaSource 创建 DataStream
            DataStreamSource<String> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source"
            );

            // 4. 预过滤 - 只保留观看事件（event_type=8）
            DataStream<String> filteredStream = kafkaStream
                    .filter(EventFilterUtil.createFastEventTypeFilter(8))
                    .name("Pre-filter View Events");

            // 5. 解析观看事件
            SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
                    .flatMap(new ViewEventParser())
                    .name("Parse View Events");

            // 6.保留最近 N 条记录
            SingleOutputStreamOperator<UserNExposures> recentStats = viewStream
                    .keyBy(event -> event.uid)
                    .process(new RecentNExposures())
                    .map(event -> event)
                    .name("recent-exposure-statistics");

            // 7. 计算色情标签并输出两路Redis写入（key1正反馈、key2负反馈）
            DataStream<RedisWriteResult> writeResultStream = recentStats
                    .map(new PornLabelCalculatorV3())
                    .name("calc-porn-label-v3");

            DataStream<Tuple2<String, byte[]>> posStream = writeResultStream
                    .filter(r -> r != null && r.writePos)
                    .map(r -> Tuple2.of(r.posKey, r.posValue.getBytes()))
                    .name("pos-redis-stream");

            DataStream<Tuple2<String, byte[]>> negStream = writeResultStream
                    .filter(r -> r != null && r.writeNeg)
                    .map(r -> Tuple2.of(r.negKey, r.negValue.getBytes()))
                    .name("neg-redis-stream");

            // 8. 创建 Redis Sink（正/负TTL分别设置）
            RedisConfig redisConfigPos = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfigPos.setTtl(REDIS_TTL_POS);

            RedisConfig redisConfigNeg = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfigNeg.setTtl(REDIS_TTL_NEG);

            RedisUtil.addRedisSink(posStream, redisConfigPos, true, 100);
            RedisUtil.addRedisSink(negStream, redisConfigNeg, true, 100);

            // 执行任务
            env.execute("UserPornLabelJobV3");

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }
    }

    /**
     * 每个标签的统计信息（窗口内/最近N条）
     */
    public static class TagStatistics implements Serializable {
        public float standingTime = 0.0f;   // 总播放时长
        public int positiveCount = 0;       // 正反馈次数
        public int negativeCount = 0;       // 负反馈次数（包含短播 & dislike）
        public int shortPlayCount = 0;      // 短播次数（<3s）
        public int dislikeCount = 0;        // dislike 次数（interaction==11）
    }

    /**
     * Redis 写入结果（正/负两路）
     */
    public static class RedisWriteResult implements Serializable {
        public boolean writePos;
        public boolean writeNeg;
        public String posKey;
        public String posValue;
        public String negKey;
        public String negValue;
    }

    /**
     * 计算色情标签并根据规则决定 key1/key2 的写入
     */
    public static class PornLabelCalculatorV3 extends RichMapFunction<UserNExposures, RedisWriteResult> {
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
        }

        @Override
        public RedisWriteResult map(UserNExposures event) throws Exception {
            long viewerId = event.viewer;
            boolean isMonitored = shouldLogDetail(viewerId);

            Map<String, TagStatistics> statsMap = aggregateStats(event, isMonitored);

            String positiveTag = pickHighestPositive(statsMap, isMonitored, viewerId);
            String negativeTag = pickLowestNegative(statsMap, isMonitored, viewerId);

            String posLabel = buildLabel(positiveTag);
            String negLabel = negativeTag == null ? null : buildLabel(negativeTag);

            String keyPos = String.format(REDIS_KEY_POS, viewerId);
            String keyNeg = String.format(REDIS_KEY_NEG, viewerId);

            String currentNegInRedis = readLabelFromRedis(keyNeg);
            String currentNegForUpdate = currentNegInRedis == null ? "u_ylevel_unk" : currentNegInRedis;

            // key1（正反馈）写入判断：需要参考 key2 当前等级
            boolean writePos = false;
            if (!"u_ylevel_unk".equals(posLabel)) {
                int posLevel = getLabelLevel(posLabel);
                int negLevel = getLabelLevel(currentNegForUpdate);
                // 仅当 key1 新等级低于 key2（更低）才更新；key2 不存在则允许更新
                if (currentNegInRedis == null || posLevel < negLevel) {
                    writePos = true;
                } else {
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] key1 不更新：posLevel={} >= negLevel={}（同级或更色情，不允许写入）", viewerId, posLevel, negLevel);
                    }
                }
            }

            // key2（负反馈）写入判断：与原始 key2 比较，只有等级更低或相等才写
            boolean writeNeg = false;
            if (negLabel != null && !"u_ylevel_unk".equals(negLabel)) {
                String existingNeg = currentNegInRedis;
                if (existingNeg == null) {
                    writeNeg = true;
                } else {
                    int newNegLevel = getLabelLevel(negLabel);
                    int oldNegLevel = getLabelLevel(existingNeg);
                    if (newNegLevel <= oldNegLevel) {
                        writeNeg = true;
                    } else if (isMonitored) {
                        LOG.info("[监控用户 {}] key2 不更新：newNegLevel={} > oldNegLevel={}", viewerId, newNegLevel, oldNegLevel);
                    }
                }
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 计算结果: posLabel={}, negLabel={}, writePos={}, writeNeg={}, keyPos={}, keyNeg={}",
                        viewerId, posLabel, negLabel, writePos, writeNeg, keyPos, keyNeg);
            }

            RedisWriteResult result = new RedisWriteResult();
            result.writePos = writePos;
            result.writeNeg = writeNeg;
            if (writePos) {
                result.posKey = keyPos;
                result.posValue = posLabel;
            }
            if (writeNeg) {
                result.negKey = keyNeg;
                result.negValue = negLabel;
            }
            return result;
        }

        private Map<String, TagStatistics> aggregateStats(UserNExposures event, boolean isMonitored) {
            Map<String, TagStatistics> statsMap = new HashMap<>();
            for (Tuple4<List<PostViewInfo>, Long, Long, String> tuple : event.firstNExposures) {
                String pornTag = tuple.f3;
                TagStatistics stats = statsMap.getOrDefault(pornTag, new TagStatistics());

                for (PostViewInfo info : tuple.f0) {
                    if (info == null) {
                        continue;
                    }
                    stats.standingTime += info.standingTime;

                    // 正反馈（沿用老规则）
                    if (info.interaction != null && !info.interaction.isEmpty()) {
                        for (int action : info.interaction) {
                            if (isPositiveAction(action)) {
                                stats.positiveCount++;
                            } else if (action == 11 || action == 7 || action == 18) { // dislike / 不感兴趣
                                stats.negativeCount++;
                                if (action == 11) {
                                    stats.dislikeCount++;
                                }
                            }
                        }
                    }

                    // 短播：播放时长 < 3s 视为负反馈
                    if (info.standingTime > 0 && info.standingTime < 3.0f) {
                        stats.shortPlayCount++;
                        stats.negativeCount++;
                    }
                }

                statsMap.put(pornTag, stats);
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 标签统计结果: {}", event.viewer, statsMap);
            }
            return statsMap;
        }

        private boolean isPositiveAction(int action) {
            // 与V2一致的正反馈集合
            return action == 1 || action == 3 || action == 5 || action == 6 || action == 9
                    || action == 10 || action == 13 || action == 15 || action == 16;
        }

        private String pickHighestPositive(Map<String, TagStatistics> statsMap, boolean isMonitored, long uid) {
            String bestTag = null;
            int bestLevel = -1;
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || CleanTag.equals(tag)) {
                    continue;
                }
                boolean positive = (s.standingTime >= 10.0f) || (s.positiveCount > 0);
                boolean noNegative = s.negativeCount <= 0;
                if (positive && noNegative) {
                    int level = getTagLevel(tag);
                    if (level > bestLevel) {
                        bestLevel = level;
                        bestTag = tag;
                    }
                }
            }
            if (isMonitored) {
                LOG.info("[监控用户 {}] 正反馈候选: bestTag={}, level={}", uid, bestTag, bestLevel);
            }
            return bestTag;
        }

        private String pickLowestNegative(Map<String, TagStatistics> statsMap, boolean isMonitored, long uid) {
            String worstTag = null;
            int worstLevel = Integer.MAX_VALUE; // 越低越好（explicit高，unk低）
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || CleanTag.equals(tag)) {
                    continue;
                }
                // 负反馈触发条件：短播次数 >=5 或 dislike >=1
                if (s.shortPlayCount >= 5 || s.dislikeCount >= 1) {
                    int level = getTagLevel(tag);
                    if (level < worstLevel) {
                        worstLevel = level;
                        worstTag = tag;
                    }
                }
            }
            if (isMonitored) {
                LOG.info("[监控用户 {}] 负反馈候选: worstTag={}, level={}", uid, worstTag, worstLevel);
            }
            return worstTag;
        }

        private String readLabelFromRedis(String key) {
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(key);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    return new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                LOG.warn("读Redis失败 key={}, err={}", key, e.getMessage());
            }
            return null;
        }
    }

    // ===== 辅助函数 =====
    private static int getTagLevel(String tag) {
        if (tag == null) return 0;
        if (tag.contains("explicit")) return 3;
        if (tag.contains("high")) return 2;
        if (tag.contains("mid")) return 1;
        return 0;
    }

    private static int getLabelLevel(String label) {
        if (label == null) return 0;
        if (label.contains("explicit")) return 3;
        if (label.contains("high")) return 2;
        if (label.contains("mid")) return 1;
        return 0;
    }

    private static String buildLabel(String tag) {
        if (tag == null || tag.isEmpty()) {
            return "u_ylevel_unk";
        }
        return "u_ylevel_" + tag;
    }

    // 前N次曝光统计输出
    public static class UserNExposures  implements Serializable {
        public long viewer;
        public List<Tuple4<List<PostViewInfo>, Long, Long, String>>  firstNExposures;  // 前N次曝光记录
        public long collectionTime;  // 收集完成时间

        public UserNExposures(long viewer, List<Tuple4<List<PostViewInfo>, Long, Long, String>> exposures) {
            this.viewer = viewer;
            if (exposures == null || exposures.isEmpty()) {
                this.firstNExposures = new ArrayList<>();
            } else {
                this.firstNExposures = exposures;
            }
            this.collectionTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return String.format("User %d collected %s exposures", viewer, firstNExposures.toString());
        }
    }

    static class RecentNExposures extends KeyedProcessFunction<Long, PostViewEvent, UserNExposures> {
        private static final int N = 30;  // 保留最近30次
        private static final int CalNum = 2;  // 满足 2 条就
        private transient ListState<Tuple4<List<PostViewInfo>, Long, Long, String>> recentViewEventState;  //
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
//            recentViewEventState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<>("recentViewEvent",
//                            org.apache.flink.api.common.typeinfo.Types.TUPLE(
//                                    org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(PostViewInfo.class)),
//                                    org.apache.flink.api.common.typeinfo.Types.LONG,
//                                    org.apache.flink.api.common.typeinfo.Types.LONG,
//                                    org.apache.flink.api.common.typeinfo.Types.STRING
//                            )
//                    )
//            );

            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1)) // 设置状态存活时间为1小时
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 使用处理时间（也可用EventTime）
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 仅在创建和写入时更新TTL
                    .cleanupFullSnapshot() // 清理策略：全量快照时清理（适用RocksDB和文件系统后端）
                    .disableCleanupInBackground() // 可选：禁用后台清理（某些场景需要）
                    .build();

            // 2. 创建状态描述符，并为其启用 TTL
            ListStateDescriptor<Tuple4<List<PostViewInfo>, Long, Long, String>> descriptor =
                    new ListStateDescriptor<>(
                            "recentViewEvent",
                            TypeInformation.of(new TypeHint<Tuple4<List<PostViewInfo>, Long, Long, String>>() {})
                    );

            descriptor.enableTimeToLive(ttlConfig); // 关键：启用TTL

            // 3. 获取状态
            recentViewEventState = getRuntimeContext().getListState(descriptor);

        }
        @Override
        public void close() throws Exception {
            if (redisManager != null) {
                redisManager.shutdown();
            }
            super.close();
        }

        @Override
        public void processElement(PostViewEvent event, Context ctx, Collector<UserNExposures> out) throws Exception {

            long currentTime = ctx.timestamp();
            long viewerId = event.uid;
            boolean isMonitored = shouldLogDetail(viewerId);

            if (isMonitored) {
                LOG.info("========== [监控用户 {}] 收到新事件 ==========", viewerId);
                LOG.info("[监控用户 {}] 事件时间戳: {}", viewerId, currentTime);
                LOG.info("[监控用户 {}] 事件创建时间: {}", viewerId, event.createdAt);
                LOG.info("[监控用户 {}] PostViewInfo 数量: {}", viewerId, event.infoList.size());
                for (int i = 0; i < event.infoList.size(); i++) {
                    PostViewInfo info = event.infoList.get(i);
                    LOG.info("[监控用户 {}]   PostViewInfo #{}: postId={}, standingTime={}, progressTime={}, author={}, recToken={}, interactions={}", 
                            viewerId, i + 1, info.postId, info.standingTime, info.progressTime, info.author, info.recToken, info.interaction);
                }
            }

            // 获取当前所有记录
            List<Tuple4<List<PostViewInfo>, Long, Long, String>> allRecords = new ArrayList<>();

            for (Tuple4<List<PostViewInfo>, Long, Long, String> record : recentViewEventState.get()) {
                allRecords.add(record);
            }

            if (isMonitored) {
                LOG.info("[监控用户 {}] 当前状态中的记录数: {}", viewerId, allRecords.size());
                for (int i = 0; i < allRecords.size(); i++) {
                    Tuple4<List<PostViewInfo>, Long, Long, String> record = allRecords.get(i);
                    LOG.info("[监控用户 {}]   状态记录 #{}: postId={}, expoTime={}, pornTag={}, PostViewInfo数量={}", 
                            viewerId, i + 1, record.f1, record.f2, record.f3, record.f0.size());
                }
            }

            // 添加新记录，包含序号
            for (PostViewInfo info : event.infoList) { // 按 postid 归类
                boolean exist = false;
                for (Tuple4<List<PostViewInfo>, Long, Long, String> record : allRecords) {
                    if (record.f1 == info.postId) { // 已存在
                        if (isMonitored) {
                            LOG.info("[监控用户 {}] PostId {} 已存在，追加 PostViewInfo", viewerId, info.postId);
                        }
                        record.f0.add(info);
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] PostId {} 不存在，从 Redis 读取标签", viewerId, info.postId);
                    }
                    String pornTag = getPostTagFromRedis(info.postId, isMonitored).get();
                    List<PostViewInfo> infos = new ArrayList<>();
                    infos.add(info);
                    allRecords.add(new Tuple4<>(
                            infos,
                            info.postId,
                            event.createdAt,
                            pornTag
                    ));
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] 新增记录: postId={}, expoTime={}, pornTag={}", 
                                viewerId, info.postId, event.createdAt, pornTag);
                    }
                }
            }

            // 按 num 排序（如果需要）
            allRecords.sort(Comparator.comparing(r -> r.f2));

            if (isMonitored) {
                LOG.info("[监控用户 {}] 排序后的记录数: {}", viewerId, allRecords.size());
                for (int i = 0; i < allRecords.size(); i++) {
                    Tuple4<List<PostViewInfo>, Long, Long, String> record = allRecords.get(i);
                    LOG.info("[监控用户 {}]   排序后记录 #{}: postId={}, expoTime={}, pornTag={}, PostViewInfo数量={}", 
                            viewerId, i + 1, record.f1, record.f2, record.f3, record.f0.size());
                }
            }

            // 只保留最近N条记录
            int originalSize = allRecords.size();
            if (allRecords.size() > N) {
                allRecords = new ArrayList<>(allRecords.subList(allRecords.size() - N, allRecords.size()));
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 记录数 {} > N({})，截取最近 {} 条", viewerId, originalSize, N, allRecords.size());
                }
            } else {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 记录数 {} <= N({})，保留全部", viewerId, allRecords.size(), N);
                }
            }

            // 更新状态
            recentViewEventState.clear();
            recentViewEventState.addAll(allRecords);

            if (isMonitored) {
                LOG.info("[监控用户 {}] 状态已更新，当前记录数: {}", viewerId, allRecords.size());
            }

            // 输出最新状态
            if (allRecords.size() >= CalNum) {
                UserNExposures result = new UserNExposures(viewerId, allRecords);
                if (isMonitored) {
                    LOG.info("[监控用户 {}] ✓ 满足输出条件 (记录数 {} >= CalNum {})，输出 UserNExposures", 
                            viewerId, allRecords.size(), CalNum);
                    LOG.info("[监控用户 {}] 输出结果: viewer={}, exposureCount={}, collectionTime={}", 
                            viewerId, result.viewer, result.firstNExposures.size(), result.collectionTime);
                }
                out.collect(result);
            } else {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] ✗ 不满足输出条件 (记录数 {} < CalNum {})，不输出", 
                            viewerId, allRecords.size(), CalNum);
                }
            }
        }
        String UNK = "unk";
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            return getPostTagFromRedis(postId, false);
        }
        
        private CompletableFuture<String> getPostTagFromRedis(long postId, boolean isMonitored) {
            String redisKey = "rec_post:{" + postId + "}:aitag";
            
            if (isMonitored) {
                LOG.info("========== [监控用户] Redis 读取 ==========");
                LOG.info("Redis Key: {}", redisKey);
            }
            
            return redisManager.executeStringAsync(
                    commands -> {
                        try {
                            Tuple2<String, byte[]> tuple = commands.get(redisKey);
                            if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                                String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                                
                                if (isMonitored) {
                                    LOG.info("Redis Value (原始): {}", value);
                                    LOG.info("Redis Value (bytes): {}", java.util.Arrays.toString(tuple.f1));
                                }
                                
                                if (value.isEmpty()) {
                                    if (isMonitored) {
                                        LOG.info("Redis Value 为空，返回默认标签: {}", UNK);
                                    }
                                    return UNK;
                                }
                                
                                String selectedTag = selectPornTag(value, postId, isMonitored);
                                if (isMonitored) {
                                    LOG.info("最终选择的标签: {}", selectedTag);
                                }
                                return selectedTag;
                            } else {
                                if (isMonitored) {
                                    LOG.info("Redis Key 不存在或值为空，返回默认标签: {}", UNK);
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to fetch tag from Redis for postId {}: {}", postId, e.getMessage());
                            if (isMonitored) {
                                LOG.error("[监控用户] Redis 读取异常，postId={}, error={}", postId, e.getMessage(), e);
                            }
                        }
                        return UNK;
                    }
            );
        }
        
        private String selectPornTag(String rawValue) {
            return selectPornTag(rawValue, -1, false);
        }
        
        private String selectPornTag(String rawValue, long postId, boolean isMonitored) {
            if (isMonitored) {
                LOG.info("========== [监控用户] 标签解析 ==========");
                LOG.info("PostId: {}", postId);
                LOG.info("原始标签字符串: {}", rawValue);
            }
            
            String[] tags = rawValue.split(",");
            String contentCandidate = UNK;
            
            if (isMonitored) {
                LOG.info("分割后的标签数组 (数量: {}): {}", tags.length, java.util.Arrays.toString(tags));
            }
            
            for (int i = 0; i < tags.length; i++) {
                String tag = tags[i];
                if (tag == null) {
                    if (isMonitored) {
                        LOG.info("  标签 #{}: null (跳过)", i + 1);
                    }
                    continue;
                }
                String trimmed = tag.trim();
                if (trimmed.isEmpty()) {
                    if (isMonitored) {
                        LOG.info("  标签 #{}: 空字符串 (跳过)", i + 1);
                    }
                    continue;
                }
                
                if (isMonitored) {
                    LOG.info("  标签 #{}: {}", i + 1, trimmed);
                }
                
                if (trimmed.contains("restricted#")) {
                    if (isMonitored) {
                        LOG.info("    包含 'restricted#'，开始解析");
                    }
                    String[] vals = trimmed.split("#");
                    if (isMonitored) {
                        LOG.info("    分割结果: {}", java.util.Arrays.toString(vals));
                    }
                    if (vals.length == 2) {
                        String tagType = vals[1];
                        if (isMonitored) {
                            LOG.info("    标签类型: {}", tagType);
                        }
                        if (CleanTag.equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 clean，返回: {}", CleanTag);
                            }
                            return CleanTag;
                        } else if ("explicit".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 explicit，返回: explicit");
                            }
                            return "explicit";
                        } else if ("borderline".equals(tagType) || "mid".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 borderline 或 mid，返回: mid");
                            }
                            return "mid";
                        } else if ("mid-sexy".equals(tagType) || "high".equals(tagType)) {
                            if (isMonitored) {
                                LOG.info("    → 匹配到 mid-sexy 或 high，返回: high");
                            }
                            return "high";
                        } else {
                            if (isMonitored) {
                                LOG.info("    → 未知标签类型: {}，返回: {}", tagType, UNK);
                            }
                            return UNK;
                        }
                    } else {
                        if (isMonitored) {
                            LOG.info("    分割后长度 != 2，跳过");
                        }
                    }
                    break;
                }
            }
            
            if (isMonitored) {
                LOG.info("未找到匹配的 restricted# 标签，返回默认: {}", contentCandidate);
            }
            return contentCandidate;
        }

    }

}