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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class UserPornLabelJobTimeWindowV0 {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJobTimeWindowV0.class);

    // Redis Key 前缀和后缀
    private static final String RedisKey = "rec_post:{%d}:rtylevel_v2";

    private static final String CleanTag = "clean";
    private static final int REDIS_TTL = 3 * 3600; // 3小时

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "rec_porn_label_v2";

    // 时间窗口配置：3分钟窗口，20秒滑动
    private static final int WINDOW_SIZE_MINUTES = 3;
    private static final int SLIDE_INTERVAL_SECONDS = 20;

    // 快滑阈值：连续快滑次数达到此值触发降级
    private static final int QUICK_SWIPE_THRESHOLD = 5;

    // 短播阈值：播放时长小于此值（秒）视为短播负反馈
    private static final float SHORT_PLAY_THRESHOLD = 3.0f;

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

            // 5. 解析观看事件并设置时间戳
            SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
                    .flatMap(new ViewEventParser())
                    .name("Parse View Events")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<PostViewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                    .withTimestampAssigner((event, recordTimestamp) -> event.createdAt)
                    );

            // 6. 按 viewer_id 分组，使用滑动窗口聚合（3分钟窗口，20秒滑动）
            // 窗口内统计每个用户对各个 pornTag 的正负反馈情况
            SingleOutputStreamOperator<WindowAggregationResult> windowAggregated = viewStream
                    .keyBy(new KeySelector<PostViewEvent, Long>() {
                        @Override
                        public Long getKey(PostViewEvent value) throws Exception {
                            return value.uid; // 按 viewer_id (uid) 分组
                        }
                    })
                    .window(SlidingProcessingTimeWindows.of(
                            Duration.ofMinutes(WINDOW_SIZE_MINUTES),
                            Duration.ofSeconds(SLIDE_INTERVAL_SECONDS)
                    ))
                    .aggregate(new PornTagWindowAggregator())
                    .name("Window Aggregation by Viewer ID");

            // 7. 计算用户色情标签，并应用升级/降级逻辑
            List<Integer> positiveActions = Arrays.asList(1, 3, 5, 6, 9, 10, 13, 15, 16); // 点赞，评论，分享，收藏, 下载, 购买, 关注或点进主页关注，查看主页，订阅
            DataStream<Tuple2<String, byte[]>> dataStream = windowAggregated
                    .map(new PornLabelCalculator(positiveActions))
                    .name("Calculate Porn Label with Upgrade/Downgrade Logic")
                    // 过滤掉 value 为 u_ylevel_unk 的记录，不写入 Redis
                    .filter(new FilterFunction<Tuple2<String, byte[]>>() {
                        @Override
                        public boolean filter(Tuple2<String, byte[]> value) throws Exception {
                            if (value == null || value.f1 == null) {
                                return false;
                            }
                            
                            // 将 byte[] 转换回 String 进行比较
                            String labelValue = new String(value.f1, java.nio.charset.StandardCharsets.UTF_8);
                            
                            // 如果是 u_ylevel_unk，过滤掉（返回 false 表示不保留）
                            if (!("u_ylevel_explicit".equals(labelValue) || "u_ylevel_high".equals(labelValue)
                                    || "u_ylevel_mid".equals(labelValue))) {
                                LOG.debug("过滤掉 非 u_ylevel_explicit｜u_ylevel_high｜u_ylevel_mid 标签，不写入 Redis: key={}", value.f0);
                                return false;
                            }
                            
                            return true;
                        }
                    })
                    .name("filter unk label");

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
            env.execute("UserPornLabelJobTimeWindowV0");

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }
    }

    /**
     * 窗口聚合结果：包含用户ID和按pornTag聚合的统计信息
     */
    public static class WindowAggregationResult implements Serializable {
        public long viewerId;
        // 按pornTag聚合的统计信息
        public Map<String, TagStatistics> tagStatistics; // pornTag -> TagStatistics
        
        public WindowAggregationResult() {
            this.tagStatistics = new HashMap<>();
        }
        
        public WindowAggregationResult(long viewerId, Map<String, TagStatistics> tagStatistics) {
            this.viewerId = viewerId;
            this.tagStatistics = tagStatistics;
        }
    }
    
    /**
     * 单个pornTag的统计信息
     */
    public static class TagStatistics implements Serializable {
        public float totalStandingTime;      // 总观看时长
        public int positiveCount;            // 正反馈次数（点赞、评论、分享等）
        public int negativeCount;            // 负反馈次数（dislike、短播、快滑等）
        public int shortPlayCount;           // 短播次数（播放时长 < 3秒）
        public int dislikeCount;              // dislike次数（interaction == 11）
        public int quickSwipeCount;           // 快滑次数（连续短播）
        
        public TagStatistics() {
            this.totalStandingTime = 0.0f;
            this.positiveCount = 0;
            this.negativeCount = 0;
            this.shortPlayCount = 0;
            this.dislikeCount = 0;
            this.quickSwipeCount = 0;
        }
    }

    /**
     * 窗口聚合器：在滑动窗口内统计每个用户对各个pornTag的正负反馈
     */
    public static class PornTagWindowAggregator implements AggregateFunction<PostViewEvent, WindowAggregationResult, WindowAggregationResult> {
        private transient RedisConnectionManager redisManager;
        
        // 延迟初始化 Redis 连接
        private RedisConnectionManager getRedisManager() {
            if (redisManager == null) {
                redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
            }
            return redisManager;
        }
        
        @Override
        public WindowAggregationResult createAccumulator() {
            return new WindowAggregationResult();
        }
        
        @Override
        public WindowAggregationResult add(PostViewEvent value, WindowAggregationResult accumulator) {
            // 设置viewerId（第一次设置）
            if (accumulator.viewerId == 0) {
                accumulator.viewerId = value.uid;
            }
            
            // 处理每个PostViewInfo
            for (PostViewInfo info : value.infoList) {
                if (info == null) {
                    continue;
                }
                
                // 从Redis获取post的pornTag
                String pornTag = getPostTagFromRedisSync(info.postId, getRedisManager());
                
                // 获取或创建该tag的统计信息
                TagStatistics stats = accumulator.tagStatistics.getOrDefault(pornTag, new TagStatistics());
                
                // 统计观看时长
                stats.totalStandingTime += info.standingTime;
                
                // 统计正反馈
                if (info.interaction != null && !info.interaction.isEmpty()) {
                    for (int action : info.interaction) {
                        if (action == 11) { // dislike
                            stats.dislikeCount++;
                            stats.negativeCount++;
                        } else if (action == 7 || action == 18) { // 其他负反馈
                            stats.negativeCount++;
                        }
                    }
                }
                
                // 统计短播（播放时长 < 3秒）
                if (info.standingTime > 0 && info.standingTime < SHORT_PLAY_THRESHOLD) {
                    stats.shortPlayCount++;
                    stats.negativeCount++;
                }
                
                // 更新统计
                accumulator.tagStatistics.put(pornTag, stats);
            }
            
            return accumulator;
        }
        
        @Override
        public WindowAggregationResult getResult(WindowAggregationResult accumulator) {
            // 计算快滑次数（连续短播）
            // 这里简化处理：如果短播次数 >= 阈值，则认为有快滑行为
            for (TagStatistics stats : accumulator.tagStatistics.values()) {
                if (stats.shortPlayCount >= QUICK_SWIPE_THRESHOLD) {
                    stats.quickSwipeCount = stats.shortPlayCount;
                }
            }
            
            return accumulator;
        }
        
        @Override
        public WindowAggregationResult merge(WindowAggregationResult a, WindowAggregationResult b) {
            // 合并两个accumulator
            WindowAggregationResult merged = new WindowAggregationResult();
            merged.viewerId = a.viewerId != 0 ? a.viewerId : b.viewerId;
            
            // 合并tag统计
            merged.tagStatistics.putAll(a.tagStatistics);
            for (Map.Entry<String, TagStatistics> entry : b.tagStatistics.entrySet()) {
                String tag = entry.getKey();
                TagStatistics bStats = entry.getValue();
                TagStatistics mergedStats = merged.tagStatistics.getOrDefault(tag, new TagStatistics());
                
                mergedStats.totalStandingTime += bStats.totalStandingTime;
                mergedStats.positiveCount += bStats.positiveCount;
                mergedStats.negativeCount += bStats.negativeCount;
                mergedStats.shortPlayCount += bStats.shortPlayCount;
                mergedStats.dislikeCount += bStats.dislikeCount;
                mergedStats.quickSwipeCount += bStats.quickSwipeCount;
                
                merged.tagStatistics.put(tag, mergedStats);
            }
            
            return merged;
        }
        
        /**
         * 同步从Redis获取post的pornTag
         */
        private String getPostTagFromRedisSync(long postId, RedisConnectionManager redisManager) {
            String redisKey = "rec_post:{" + postId + "}:aitag";
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(redisKey);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                    if (!value.isEmpty()) {
                        return selectPornTag(value);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to fetch tag from Redis for postId {}: {}", postId, e.getMessage());
            }
            return "unk";
        }
        
        /**
         * 从Redis value中提取pornTag
         */
        private String selectPornTag(String rawValue) {
            String[] tags = rawValue.split(",");
            for (String tag : tags) {
                if (tag == null || tag.trim().isEmpty()) {
                    continue;
                }
                String trimmed = tag.trim();
                if (trimmed.contains("restricted#")) {
                    String[] vals = trimmed.split("#");
                    if (vals.length == 2) {
                        String tagType = vals[1];
                        if (CleanTag.equals(tagType)) {
                            return CleanTag;
                        } else if ("explicit".equals(tagType)) {
                            return "explicit";
                        } else if ("borderline".equals(tagType) || "mid".equals(tagType)) {
                            return "mid";
                        } else if ("mid-sexy".equals(tagType) || "high".equals(tagType)) {
                            return "high";
                        }
                    }
                    break;
                }
            }
            return "unk";
        }
    }

    /**
     * 标签计算器：根据窗口聚合结果计算用户色情标签，并应用升级/降级逻辑
     */
    public static class PornLabelCalculator extends RichMapFunction<WindowAggregationResult, Tuple2<String, byte[]>> {
        private transient RedisConnectionManager redisManager;
        
        public PornLabelCalculator(List<Integer> positiveActions) {
            // positiveActions 参数保留用于未来扩展，当前未使用
        }
        
        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
        }
        
        @Override
        public Tuple2<String, byte[]> map(WindowAggregationResult aggregation) throws Exception {
            long viewerId = aggregation.viewerId;
            boolean isMonitored = shouldLogDetail(viewerId);
            
            if (isMonitored) {
                LOG.info("========== [监控用户 {}] 开始计算色情标签 ==========", viewerId);
                LOG.info("[监控用户 {}] 窗口内标签数量: {}", viewerId, aggregation.tagStatistics.size());
            }
            
            // 1. 根据窗口聚合结果计算新标签
            String newLabel = calculateNewLabel(aggregation, isMonitored);
            
            // 2. 从Redis读取当前标签
            String redisKey = String.format(RedisKey, viewerId);
            String currentLabel = getCurrentLabelFromRedis(redisKey, isMonitored);
            
            if (isMonitored) {
                LOG.info("[监控用户 {}] 当前Redis标签: {}, 计算出的新标签: {}", viewerId, currentLabel, newLabel);
            }
            
            // 3. 应用升级/降级逻辑
            String finalLabel = applyUpgradeDowngradeLogic(currentLabel, newLabel, aggregation, isMonitored);
            
            if (isMonitored) {
                LOG.info("[监控用户 {}] ========== Redis 写入信息 ==========", viewerId);
                LOG.info("[监控用户 {}] Redis Key: {}", viewerId, redisKey);
                LOG.info("[监控用户 {}] Redis Value: {}", viewerId, finalLabel);
                LOG.info("[监控用户 {}] Redis TTL: {} 秒 ({} 小时)", viewerId, REDIS_TTL, REDIS_TTL / 3600);
                LOG.info("[监控用户 {}] ========== Redis 写入完成 ==========", viewerId);
            } else {
                LOG.info("数据写入: {} -> {}", redisKey, finalLabel);
            }
            
            return new Tuple2<>(redisKey, finalLabel.getBytes());
        }
        
        /**
         * 根据窗口聚合结果计算新标签
         */
        private String calculateNewLabel(WindowAggregationResult aggregation, boolean isMonitored) {
            long viewerId = aggregation.viewerId;
            
            // 按观看时长排序
            List<Map.Entry<String, TagStatistics>> sortedTags = new ArrayList<>(aggregation.tagStatistics.entrySet());
            sortedTags.sort((e1, e2) -> Float.compare(e2.getValue().totalStandingTime, e1.getValue().totalStandingTime));
            
            if (isMonitored) {
                LOG.info("[监控用户 {}] ========== 标签排序结果（按观看时长降序）==========", viewerId);
                for (int i = 0; i < sortedTags.size(); i++) {
                    Map.Entry<String, TagStatistics> entry = sortedTags.get(i);
                    TagStatistics stats = entry.getValue();
                    LOG.info("[监控用户 {}] 排序 #{}: 标签={}, 观看时长={}, 正反馈数={}, 负反馈数={}, 短播数={}, dislike数={}", 
                            viewerId, i + 1, entry.getKey(), stats.totalStandingTime, 
                            stats.positiveCount, stats.negativeCount, stats.shortPlayCount, stats.dislikeCount);
                }
            }
            
            // 查找匹配的标签
            for (Map.Entry<String, TagStatistics> entry : sortedTags) {
                String tag = entry.getKey();
                TagStatistics stats = entry.getValue();
                
                if ("unk".equals(tag) || CleanTag.equals(tag)) {
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] 跳过标签 [{}] (unk 或 clean)", viewerId, tag);
                    }
                    continue;
                }
                
                // 判断条件：观看时长 >= 10秒 或 有正反馈，且没有负反馈
                boolean condition1 = (stats.totalStandingTime >= 10.0f || stats.positiveCount > 0);
                boolean condition2 = (stats.negativeCount <= 0);
                boolean matched = condition1 && condition2;
                
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 检查标签 [{}]: standingTime={} >= 10 或 positiveCount={} > 0 ? {}, negativeCount={} <= 0 ? {}, 匹配结果: {}",
                            viewerId, tag, stats.totalStandingTime, stats.positiveCount, condition1, 
                            stats.negativeCount, condition2, matched);
                }
                
                if (matched) {
                    String label = "u_ylevel_" + tag;
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] ✓ 匹配成功！新标签: {}", viewerId, label);
                    }
                    return label;
                }
            }
            
            // 默认返回unk
            if (isMonitored) {
                LOG.info("[监控用户 {}] 未找到匹配标签，返回默认: u_ylevel_unk", viewerId);
            }
            return "u_ylevel_unk";
        }
        
        /**
         * 从Redis读取当前标签
         */
        private String getCurrentLabelFromRedis(String redisKey, boolean isMonitored) {
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(redisKey);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                    if (isMonitored) {
                        LOG.info("[监控用户] Redis读取: key={}, value={}", redisKey, value);
                    }
                    return value;
                }
            } catch (Exception e) {
                LOG.warn("Failed to read current label from Redis for key {}: {}", redisKey, e.getMessage());
            }
            return "u_ylevel_unk"; // 默认值
        }
        
        /**
         * 应用升级/降级逻辑
         * 
         * 升级规则：如果新标签等级更高，则更新（等级：explicit > high > mid > unk）
         * 降级规则：如果满足降级条件（dislike或快滑5次以上），则降2级
         *   - explicit -> mid
         *   - high -> unk
         *   - mid -> unk
         */
        private String applyUpgradeDowngradeLogic(String currentLabel, String newLabel, 
                                                   WindowAggregationResult aggregation, boolean isMonitored) {
            long viewerId = aggregation.viewerId;
            
            // 检查是否满足降级条件
            boolean shouldDowngrade = false;
            for (TagStatistics stats : aggregation.tagStatistics.values()) {
                // 条件1：有dislike
                if (stats.dislikeCount > 0) {
                    shouldDowngrade = true;
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] 检测到dislike，触发降级", viewerId);
                    }
                    break;
                }
                // 条件2：快滑5次以上
                if (stats.quickSwipeCount >= QUICK_SWIPE_THRESHOLD) {
                    shouldDowngrade = true;
                    if (isMonitored) {
                        LOG.info("[监控用户 {}] 检测到快滑{}次，触发降级", viewerId, stats.quickSwipeCount);
                    }
                    break;
                }
            }
            
            if (shouldDowngrade) {
                // 执行降级：降2级
                String downgradedLabel = downgradeLabel(currentLabel, isMonitored);
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 降级: {} -> {}", viewerId, currentLabel, downgradedLabel);
                }
                return downgradedLabel;
            }
            
            // 升级逻辑：比较等级，只升级不降级
            int currentLevel = getLabelLevel(currentLabel);
            int newLevel = getLabelLevel(newLabel);
            
            if (isMonitored) {
                LOG.info("[监控用户 {}] 等级比较: 当前={}(level {}), 新标签={}(level {})", 
                        viewerId, currentLabel, currentLevel, newLabel, newLevel);
            }
            
            // 如果新标签等级更高，则升级
            if (newLevel > currentLevel) {
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 升级: {} -> {}", viewerId, currentLabel, newLabel);
                }
                return newLabel;
            } else {
                // 保持当前标签（不降级）
                if (isMonitored) {
                    LOG.info("[监控用户 {}] 保持当前标签: {}", viewerId, currentLabel);
                }
                return currentLabel;
            }
        }
        
        /**
         * 获取标签等级（数字越大等级越高）
         * explicit=3, high=2, mid=1, unk=0
         */
        private int getLabelLevel(String label) {
            if (label == null) {
                return 0;
            }
            if (label.contains("explicit")) {
                return 3;
            } else if (label.contains("high")) {
                return 2;
            } else if (label.contains("mid")) {
                return 1;
            } else {
                return 0; // unk
            }
        }
        
        /**
         * 降级标签（降2级）
         * explicit -> mid
         * high -> unk
         * mid -> unk
         */
        private String downgradeLabel(String label, boolean isMonitored) {
            if (label == null) {
                return "u_ylevel_unk";
            }
            if (label.contains("explicit")) {
                return "u_ylevel_mid";
            } else if (label.contains("high")) {
                return "u_ylevel_unk";
            } else if (label.contains("mid")) {
                return "u_ylevel_unk";
            } else {
                return "u_ylevel_unk";
            }
        }
    }

    // 注意：旧的 RecentNExposures 类已被窗口聚合器 PornTagWindowAggregator 替代
    // 以下代码已废弃，保留仅用于参考
    /*
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
    */

}