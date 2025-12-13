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
import org.apache.flink.api.common.functions.MapFunction;
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

public class UserPornLabelJobV2 {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJobV2.class);

    // Redis Key 前缀和后缀
    private static final String RedisKey = "rec_post:{%d}:rtylevel_v2";

    private static final String CleanTag = "clean";
    private static final int REDIS_TTL = 3 * 3600; // 3小时

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "rec_porn_label_v2";

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
                    .map(event -> {
                        return event;
                    })
                    .name("recent-exposure-statistics");
            //7. 计算 用户 属于的色情群体
            List<Integer> positiveActions = Arrays.asList(1,3,5,6,9,10,13,15,16); // 点赞，评论，分享，收藏, 下载, 购买, 关注或点进主页关注，查看主页，订阅
            DataStream<Tuple2<String, byte[]>> dataStream =recentStats
                    .map(new MapFunction<UserNExposures, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(UserNExposures event) throws Exception {
                            long viewerId = event.viewer;
                            boolean isMonitored = shouldLogDetail(viewerId);
                            
                            if (isMonitored) {
                                LOG.info("========== [监控用户 {}] 开始计算色情标签 ==========", viewerId);
                                LOG.info("[监控用户 {}] 曝光记录数量: {}", viewerId, event.firstNExposures.size());
                                LOG.info("[监控用户 {}] 收集时间: {}", viewerId, new java.util.Date(event.collectionTime));
                            }
                            
                            String pornLabel = getPornLabel(event, positiveActions, isMonitored);
                            
                            // 构建 Redis key
                            String redisKey = String.format(RedisKey, event.viewer);
                            
                            if (isMonitored) {
                                LOG.info("========== [监控用户 {}] Redis 写入信息 ==========", viewerId);
                                LOG.info("[监控用户 {}] Redis Key: {}", viewerId, redisKey);
                                LOG.info("[监控用户 {}] Redis Value: {}", viewerId, pornLabel);
                                LOG.info("[监控用户 {}] Redis Value (bytes): {}", viewerId, java.util.Arrays.toString(pornLabel.getBytes()));
                                LOG.info("[监控用户 {}] Redis TTL: {} 秒 ({} 小时)", viewerId, REDIS_TTL, REDIS_TTL / 3600);
                                if (!event.firstNExposures.isEmpty()) {
                                    LOG.info("[监控用户 {}] 最早曝光时间: {}", viewerId, event.firstNExposures.get(0).f2);
                                }
                                LOG.info("========== [监控用户 {}] Redis 写入完成 ==========", viewerId);
                            } else {
                                if  (!event.firstNExposures.isEmpty()) {
                                    LOG.info("数据写入: {} -> {} time {}", redisKey, pornLabel, event.firstNExposures.get(0).f2);
                                } else {
                                    LOG.info("数据写入: {} -> {}", redisKey, pornLabel);
                                }
                            }

                            return new Tuple2<>(redisKey, pornLabel.getBytes());
                        }
                    })
                    .name("cal user porn label");

            // 8. 创建 Redis Sink
            RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfig.setTtl(REDIS_TTL);

            RedisUtil.addRedisSink(
                    dataStream,
                    redisConfig,
                    true,
                    100
            );

            // 执行任务
            env.execute("UserPornLabelJobV2");

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }
    }

    public static String getPornLabel(UserNExposures event, List<Integer> positiveActions) {
        return getPornLabel(event, positiveActions, false);
    }

    public static String getPornLabel(UserNExposures event, List<Integer> positiveActions, boolean isMonitored) {
        long viewerId = event.viewer;
        
        if (isMonitored) {
            LOG.info("[监控用户 {}] ========== 开始计算色情标签 ==========", viewerId);
            LOG.info("[监控用户 {}] 正反馈行为列表: {}", viewerId, positiveActions);
        }
        
        Map<String, Float> standingStatistics = new HashMap<>(); //pornLabel -> standingTime
        Map<String, Integer> positiveStatistics = new HashMap<>(); //pornLabel -> positiveCount
        Map<String, Integer> negativeStatistics = new HashMap<>(); //pornLabel -> negativeCount

        float allStandTime = 0.0f;
        int exposureIndex = 0;
        
        for (Tuple4<List<PostViewInfo>, Long, Long, String> tuple : event.firstNExposures) {
            exposureIndex++;
            long postId = tuple.f1;
            long expoTime = tuple.f2;
            String pornTag = tuple.f3;
            float standingTime = 0.0f;
            int positiveCount = 0;
            int negativeCount = 0;
            List<Integer> allInteractions = new ArrayList<>();

            if (isMonitored) {
                LOG.info("[监控用户 {}] --- 曝光记录 #{} ---", viewerId, exposureIndex);
                LOG.info("[监控用户 {}] PostId: {}, 曝光时间: {}, 色情标签: {}", viewerId, postId, expoTime, pornTag);
                LOG.info("[监控用户 {}] PostViewInfo 数量: {}", viewerId, tuple.f0.size());
            }

            for (PostViewInfo info : tuple.f0) {
                if (info != null) {
                    standingTime += info.standingTime;
                    if (info.interaction != null && !info.interaction.isEmpty()) {
                        for (int action : info.interaction) {
                            allInteractions.add(action);
                            if (positiveActions.contains(action)) {
                                positiveCount++;
                            } else if (action == 11 || action == 7 || action==18) { // 不感兴趣
                                negativeCount++;
                            }
                        }
                    }
                    
                    if (isMonitored) {
                        LOG.info("[监控用户 {}]   PostViewInfo: postId={}, standingTime={}, progressTime={}, interactions={}", 
                                viewerId, info.postId, info.standingTime, info.progressTime, info.interaction);
                    }
                }
            }
            
            if (isMonitored) {
                LOG.info("[监控用户 {}]   该曝光统计: standingTime={}, positiveCount={}, negativeCount={}, interactions={}", 
                        viewerId, standingTime, positiveCount, negativeCount, allInteractions);
            }
            
            allStandTime += standingTime;
            float stTime = standingStatistics.getOrDefault(pornTag, 0.0f);
            standingStatistics.put(pornTag, stTime + standingTime);
            int pCount = positiveStatistics.getOrDefault(pornTag, 0);
            positiveStatistics.put(pornTag, pCount + positiveCount);
            int negCount = negativeStatistics.getOrDefault(pornTag, 0);
            negativeStatistics.put(pornTag, negCount + negativeCount);
        }
        
        if (isMonitored) {
            LOG.info("[监控用户 {}] ========== 按标签聚合统计 ==========", viewerId);
            LOG.info("[监控用户 {}] 总观看时长: {}", viewerId, allStandTime);
            for (Map.Entry<String, Float> entry : standingStatistics.entrySet()) {
                String tag = entry.getKey();
                LOG.info("[监控用户 {}] 标签 [{}]: standingTime={}, positiveCount={}, negativeCount={}", 
                        viewerId, tag, entry.getValue(), 
                        positiveStatistics.getOrDefault(tag, 0), 
                        negativeStatistics.getOrDefault(tag, 0));
            }
        }
        
        List<Tuple2<String, Float>> standingStatisticsList = new ArrayList<>(); // <pornLabel, standingTime>
        for (Map.Entry<String, Float> entry : standingStatistics.entrySet()) {
            standingStatisticsList.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
        standingStatisticsList.sort((o1, o2) -> o2.f1.compareTo(o1.f1)); // 按 standing time 从大到小排序

        if (isMonitored) {
            LOG.info("[监控用户 {}] ========== 标签排序结果（按观看时长降序）==========", viewerId);
            for (int i = 0; i < standingStatisticsList.size(); i++) {
                Tuple2<String, Float> item = standingStatisticsList.get(i);
                LOG.info("[监控用户 {}] 排序 #{}: 标签={}, 观看时长={}, 正反馈数={}, 负反馈数={}", 
                        viewerId, i + 1, item.f0, item.f1, 
                        positiveStatistics.getOrDefault(item.f0, 0), 
                        negativeStatistics.getOrDefault(item.f0, 0));
            }
        }

        String pornLabel = "u_ylevel_unk";
        if (isMonitored) {
            LOG.info("[监控用户 {}] ========== 标签匹配规则判断 ==========", viewerId);
            LOG.info("[监控用户 {}] 默认标签: {}", viewerId, pornLabel);
        }
        
        for (Tuple2<String, Float> item : standingStatisticsList) {
            String tag = item.f0;
            float stTime = item.f1;
            int posCount = positiveStatistics.getOrDefault(tag, 0);
            int negCount = negativeStatistics.getOrDefault(tag, 0);
            
            if (item.f0.equals("unk") | item.f0.equals(CleanTag)) {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 跳过标签 [{}] (unk 或 clean)", viewerId, tag);
                }
                continue;
            }
            
            boolean condition1 = (stTime >= 30 | posCount > 0);
            boolean condition2 = (negCount <= 0);
            boolean matched = condition1 & condition2;
            
            if (isMonitored) {
                LOG.info("[监控用户 {}] 检查标签 [{}]: standingTime={} >= 30 或 positiveCount={} > 0 ? {}, negativeCount={} <= 0 ? {}, 匹配结果: {}", 
                        viewerId, tag, stTime, posCount, condition1, negCount, condition2, matched);
            }
            
            if (matched) {
                pornLabel = "u_ylevel_" + item.f0;
                if (isMonitored) {
                    LOG.info("[监控用户 {}] ✓ 匹配成功！最终标签: {}", viewerId, pornLabel);
                }
                break;
            }
        }
        
        if (isMonitored) {
            LOG.info("[监控用户 {}] ========== 最终计算结果 ==========", viewerId);
            LOG.info("[监控用户 {}] 最终色情标签: {}", viewerId, pornLabel);
        }
        
        return pornLabel;
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
        private static final int N = 10;  // 保留最近20次
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