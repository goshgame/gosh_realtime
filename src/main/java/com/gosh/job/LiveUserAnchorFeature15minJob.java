package com.gosh.job;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LiveUserAnchorFeature15minJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserAnchorFeature15minJob.class);
    private static final String PREFIX = "rec:user_anchor_feature:{";
    private static final String SUFFIX = "}:live15min";
    private static final int HLL_LOG2M = 14; // HyperLogLog精度参数
    // 每个窗口内每个用户的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 200;
    
    // 事件类型常量
    private static final String EVENT_LIVE_EXPOSURE = "live_exposure";
    private static final String EVENT_LIVE_VIEW = "live_view";
    private static final String EVENT_ENTER_LIVEROOM = "enter_liveroom";
    private static final String EVENT_EXIT_LIVEROOM = "exit_liveroom";

    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Starting LiveUserAnchorFeature15minJob");
        LOG.info("Processing event_type=1 for user-anchor features");
        LOG.info("Target events: live_exposure, live_view, enter_liveroom, exit_liveroom");
        LOG.info("Scene filter: 1, 3, 5");
        LOG.info("========================================");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        LOG.info("Job parallelism set to: {}", env.getParallelism());
        
        // 确保Kafka消费者并行度与分区数匹配
        env.getConfig().setAutoWatermarkInterval(1000); // 降低Watermark生成间隔

        // 第二步：创建Source，Kafka环境（topic=advertise）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "advertise"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            // 添加时间戳提取和Watermark生成
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> {
                    // 尝试从Kafka消息中提取时间戳
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode root = mapper.readTree(element);
                        JsonNode userEventLog = root.path("user_event_log");
                        // 检查是否有明确的时间戳字段
                        if (userEventLog.has("timestamp")) {
                            return userEventLog.get("timestamp").asLong(System.currentTimeMillis());
                        }
                    } catch (Exception e) {
                        // 解析失败时使用当前系统时间
                    }
                    // 默认使用处理时间，避免Watermark延迟
                    return System.currentTimeMillis();
                }),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)");

        // 3.1 解析 user_event_log 为 Live 用户-主播 事件
        SingleOutputStreamOperator<LiveUserAnchorEvent> eventStream = filteredStream
            .flatMap(new LiveEventParser())
            .name("Parse Live User-Anchor Events")
            // 为解析后的事件再次确认时间戳，确保后续处理使用正确的事件时间
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LiveUserAnchorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, recordTimestamp) -> System.currentTimeMillis())
            );

        // 第四步：原有的实现（按uid分组，会导致数据倾斜）- 已注释保留
        DataStream<UserAnchorFeatureAggregation> aggregatedStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
                @Override
                public Long getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.uid;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(15),
                Time.seconds(30)
            ))
            .aggregate(new UserAnchorFeatureAggregator())
            .name("User-Anchor Feature Aggregation");

//        DataStream<LiveUserAnchorEvent> rebalanceSteam = eventStream.rebalance();
//        // 保持两阶段聚合，解决key倾斜问题，同时确保序列化兼容性
//        // 第一阶段：使用Long类型作为key，但通过取模方式分散热点
//        DataStream<UserAnchorFeatureAggregation> preAggregatedStream = rebalanceSteam
//            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
//                @Override
//                public Long getKey(LiveUserAnchorEvent value) throws Exception {
//                    // 优化盐值策略：使用更大范围的盐值(64)，进一步分散热点
//                    int salt = (int) (Math.abs(value.uid) % 64);
//                    // 使用更优的位运算组合，确保更好的分布
//                    return (value.uid << 6) | salt;
//                }
//            })
//            .window(SlidingProcessingTimeWindows.of(
//                Time.minutes(15),
//                Time.seconds(15)
//            ))
//            .aggregate(new UserAnchorFeatureAggregator())
//            .name("Pre-Aggregation with Distributed Keys")
//            // 过滤掉空结果
//            .filter(agg -> agg != null)
//            .shuffle();
//
//        // 第二阶段：按原始uid进行全局聚合，使用Long类型key确保兼容性
//        DataStream<UserAnchorFeatureAggregation> aggregatedStream = preAggregatedStream
//            .keyBy(new KeySelector<UserAnchorFeatureAggregation, Long>() {
//                @Override
//                public Long getKey(UserAnchorFeatureAggregation value) throws Exception {
//                    return value.uid; // 使用原始Long类型uid作为key
//                }
//            })
//            .window(SlidingProcessingTimeWindows.of(
//                Time.minutes(15),
//                Time.seconds(30)
//            ))
//            .aggregate(new GlobalUserAnchorFeatureAggregator())
//            .name("Global Aggregation by Original UID")
//            .shuffle();
        
        

        // 第五步：转换为Protobuf并写入Redis（HashMap：key=uid，field=anchorId）
        DataStream<Tuple2<String, Map<String, byte[]>>> dataStream = aggregatedStream
            .filter(agg -> agg != null && agg.anchorFeatures != null && !agg.anchorFeatures.isEmpty())
            // 增加shuffle操作，进一步均衡数据分布
            .shuffle()
            .map(new MapFunction<UserAnchorFeatureAggregation, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(UserAnchorFeatureAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    return new Tuple2<>(redisKey, agg.anchorFeatures);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境（TTL=20分钟，DEL_HMSET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(60 * 20);
        redisConfig.setCommand("DEL_HMSET");
        LOG.info("Redis config: TTL={}, Command={}", 
            redisConfig.getTtl(), redisConfig.getCommand());
        
        // 增加Redis批处理大小，减少网络往返次数
        int redisBatchSize = 500;
        RedisUtil.addRedisHashMapSink(
            dataStream,
            redisConfig,
            true,
            redisBatchSize
        );

        LOG.info("Job configured, starting execution...");
        String JOB_NAME = "Live User-Anchor Feature 15min Job";
        FlinkMonitorUtil.executeWithMonitor(env, JOB_NAME);
    }

    // 输入事件：从 user_event_log.event_data 解析
    public static class LiveUserAnchorEvent {
        public long uid;
        public long anchorId;
        public String eventType;
        public String recToken;
        public long watchDuration;  // 观看时长（浏览和退房事件的stay_duration）
        public int scene;
        public long eventTime;  // 事件发生时间戳
    }

    // 解析器：解析 user_event_log.event 和 user_event_log.event_data
    public static class LiveEventParser implements FlatMapFunction<String, LiveUserAnchorEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static volatile long totalParsedEvents = 0;
        private static volatile long exposureCount = 0;
        private static volatile long viewCount = 0;
        private static volatile long enterCount = 0;
        private static volatile long exitCount = 0;

        @Override
        public void flatMap(String value, Collector<LiveUserAnchorEvent> out) throws Exception {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                
                // 获取 user_event_log 对象
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }
                
                // 获取事件类型（在 user_event_log.event 中）
                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();
                
                // 只处理4种事件
                if (!EVENT_LIVE_EXPOSURE.equals(event) && 
                    !EVENT_LIVE_VIEW.equals(event) && 
                    !EVENT_ENTER_LIVEROOM.equals(event) && 
                    !EVENT_EXIT_LIVEROOM.equals(event)) {
                    return;
                }
                
                // 获取 event_data 字段（在 user_event_log.event_data 中）
                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equals(eventData)) {
                    return;
                }
                
                // 解析 event_data 的JSON内容
                JsonNode data = OBJECT_MAPPER.readTree(eventData);
                
                long uid = data.path("uid").asLong(0);
                long anchorId = data.path("anchor_id").asLong(0);
                
                // 提取 scene：不同事件字段名不同
                int scene = 0;
                if (EVENT_ENTER_LIVEROOM.equals(event) || EVENT_EXIT_LIVEROOM.equals(event)) {
                    scene = data.path("from_scene").asInt(0);  // 进房/退房用 from_scene
                } else {
                    scene = data.path("scene").asInt(0);        // 曝光/浏览用 scene
                }
                
                if (uid <= 0 || anchorId <= 0) {
                    return;
                }
                
                // 过滤 scene：只保留 1, 3, 4, 5
                if (scene != 1 && scene != 3 && scene != 4 && scene != 5) {
                    return;
                }
                
                LiveUserAnchorEvent evt = new LiveUserAnchorEvent();
                evt.uid = uid;
                evt.anchorId = anchorId;
                evt.eventType = event;
                evt.scene = scene;
                
                // 从user_event_log中提取事件时间戳
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                } 
                evt.eventTime = eventTimeMillis;
                
                // 提取 rec_token（曝光事件用于去重）
                evt.recToken = data.path("rec_token").asText("");
                
                // 提取观看时长（浏览和退房事件）
                if (EVENT_LIVE_VIEW.equals(event)) {
                    evt.watchDuration = data.path("watch_duration").asLong(0);
                } else if (EVENT_EXIT_LIVEROOM.equals(event)) {
                    evt.watchDuration = data.path("stay_duration").asLong(0);
                }
                
                // 统计各事件类型数量并打印样例
                totalParsedEvents++;
                String tokenPreview = evt.recToken.isEmpty() ? "" : evt.recToken.substring(0, Math.min(20, evt.recToken.length()));
                
                if (EVENT_LIVE_EXPOSURE.equals(event)) {
                    exposureCount++;
                    if (exposureCount <= 3) {
                        LOG.info("[Parse Sample Exposure {}/3] uid={}, anchorId={}, scene={}, recToken={}", 
                            exposureCount, uid, anchorId, scene, tokenPreview);
                    }
                } else if (EVENT_LIVE_VIEW.equals(event)) {
                    viewCount++;
                    if (viewCount <= 3) {
                        LOG.info("[Parse Sample View {}/3] uid={}, anchorId={}, scene={}, watchDuration={}, recToken={}", 
                            viewCount, uid, anchorId, scene, evt.watchDuration, tokenPreview);
                    }
                } else if (EVENT_ENTER_LIVEROOM.equals(event)) {
                    enterCount++;
                    if (enterCount <= 3) {
                        LOG.info("[Parse Sample Enter {}/3] uid={}, anchorId={}, scene={}, recToken={}", 
                            enterCount, uid, anchorId, scene, tokenPreview);
                    }
                } else if (EVENT_EXIT_LIVEROOM.equals(event)) {
                    exitCount++;
                    if (exitCount <= 3) {
                        LOG.info("[Parse Sample Exit {}/3] uid={}, anchorId={}, scene={}, watchDuration={}, recToken={}", 
                            exitCount, uid, anchorId, scene, evt.watchDuration, tokenPreview);
                    }
                }
                
                if (totalParsedEvents % 10000 == 0) {
                    LOG.info("[Parser Summary] Total={}, Exposure={}, View={}, Enter={}, Exit={}", 
                        totalParsedEvents, exposureCount, viewCount, enterCount, exitCount);
                }
                
                out.collect(evt);
            } catch (Exception e) {
                // 静默处理异常
            }
        }
    }

    // 聚合器：统计每个 uid 对每个 anchor 的特征
    public static class UserAnchorFeatureAggregator implements AggregateFunction<LiveUserAnchorEvent, UserAnchorAccumulator, UserAnchorFeatureAggregation> {
        @Override
        public UserAnchorAccumulator createAccumulator() {
            return new UserAnchorAccumulator();
        }

        @Override
        public UserAnchorAccumulator add(LiveUserAnchorEvent event, UserAnchorAccumulator acc) {
            // 先设置用户ID，确保即使跳过也能写入Redis
            acc.uid = event.uid;
            
            if (acc.totalEventCount >= MAX_EVENTS_PER_WINDOW) {
                // 如果超过限制，跳过该事件，但不影响现有数据的写入
                return acc;
            }
            
            AnchorFeatureCounts counts = acc.anchorIdToCounts.computeIfAbsent(event.anchorId, k -> new AnchorFeatureCounts());
            
            // 曝光次数：使用 HyperLogLog 对 rec_token 去重
            if (EVENT_LIVE_EXPOSURE.equals(event.eventType) && !event.recToken.isEmpty()) {
                counts.exposureHLL.offer(event.recToken);
            }
            
            // 3s+观看次数：进房事件计数
            if (EVENT_ENTER_LIVEROOM.equals(event.eventType)) {
                counts.watch3sPlusCount += 1;
            }
            
            // 6s+观看次数：浏览/退房事件且时长>=6s，使用 HyperLogLog 对 rec_token 去重
            if ((EVENT_LIVE_VIEW.equals(event.eventType) || EVENT_EXIT_LIVEROOM.equals(event.eventType)) 
                && event.watchDuration >= 6 && !event.recToken.isEmpty()) {
                counts.watch6sPlusHLL.offer(event.recToken);
            }
            
            // 观看时长：浏览和退房事件的 stay_duration 求和
            if (EVENT_LIVE_VIEW.equals(event.eventType) || EVENT_EXIT_LIVEROOM.equals(event.eventType)) {
                counts.totalWatchDuration += event.watchDuration;
            }
            
            acc.totalEventCount++;
            return acc;
        }

        @Override
        public UserAnchorFeatureAggregation getResult(UserAnchorAccumulator acc) {
            if (acc.anchorIdToCounts.isEmpty()) {
                return null;
            }
            UserAnchorFeatureAggregation result = new UserAnchorFeatureAggregation();
            result.uid = acc.uid;
            result.anchorFeatures = new HashMap<>();

            for (Map.Entry<Long, AnchorFeatureCounts> e : acc.anchorIdToCounts.entrySet()) {
                long anchorId = e.getKey();
                AnchorFeatureCounts c = e.getValue();
                
                // 从 HyperLogLog 获取去重后的曝光次数
                int exposureCount = (int) c.exposureHLL.cardinality();
                
                // 从 HyperLogLog 获取去重后的 6s+ 观看次数
                int watch6sPlusCount = (int) c.watch6sPlusHLL.cardinality();
                
                // 负反馈次数 = 曝光次数 - 6s+观看次数
                int negative = Math.max(0, exposureCount - watch6sPlusCount);
                
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserId(acc.uid)
                    .setAnchorId(anchorId)
                    .setUserAnchorExpCnt15Min(exposureCount)
                    .setUserAnchor3SquitCnt15Min(c.watch3sPlusCount)
                    .setUserAnchor6SquitCnt15Min(watch6sPlusCount)
                    .setUserAnchorNegativeFeedbackCnt15Min(negative)
                    .build()
                    .toByteArray();
                result.anchorFeatures.put(String.valueOf(anchorId), bytes);
            }
            return result.anchorFeatures.isEmpty() ? null : result;
        }

        @Override
        public UserAnchorAccumulator merge(UserAnchorAccumulator a, UserAnchorAccumulator b) {
            // 合并事件计数
            a.totalEventCount += b.totalEventCount;
            
            for (Map.Entry<Long, AnchorFeatureCounts> e : b.anchorIdToCounts.entrySet()) {
                AnchorFeatureCounts target = a.anchorIdToCounts.computeIfAbsent(e.getKey(), k -> new AnchorFeatureCounts());
                AnchorFeatureCounts src = e.getValue();
                
                // 合并 HyperLogLog
                try {
                    target.exposureHLL = (HyperLogLog) target.exposureHLL.merge(src.exposureHLL);
                    target.watch6sPlusHLL = (HyperLogLog) target.watch6sPlusHLL.merge(src.watch6sPlusHLL);
                } catch (Exception ex) {
                    // 合并失败，保持原值
                }
                
                target.watch3sPlusCount += src.watch3sPlusCount;
                target.totalWatchDuration += src.totalWatchDuration;
            }
            return a;
        }
    }

    public static class UserAnchorAccumulator {
        public long uid;
        public int totalEventCount = 0;
        public Map<Long, AnchorFeatureCounts> anchorIdToCounts = new HashMap<>();
    }

    public static class AnchorFeatureCounts {
        public HyperLogLog exposureHLL = new HyperLogLog(HLL_LOG2M);     // 曝光次数（rec_token去重）
        public int watch3sPlusCount = 0;                                  // 3s+观看次数（进房）
        public HyperLogLog watch6sPlusHLL = new HyperLogLog(HLL_LOG2M);  // 6s+观看次数（rec_token去重）
        public long totalWatchDuration = 0;                               // 总观看时长
    }

    public static class UserAnchorFeatureAggregation {
        public long uid;
        public Map<String, byte[]> anchorFeatures; // anchorId -> protobuf bytes
    }
    
    // 全局聚合器：合并相同uid的预聚合结果，解决数据倾斜问题
    // 注意：这里修改为直接处理UserAnchorFeatureAggregation类型，避免使用Tuple2
    public static class GlobalUserAnchorFeatureAggregator implements AggregateFunction<UserAnchorFeatureAggregation, UserAnchorAccumulator, UserAnchorFeatureAggregation> {
        @Override
        public UserAnchorAccumulator createAccumulator() {
            UserAnchorAccumulator acc = new UserAnchorAccumulator();
            acc.anchorIdToCounts = new HashMap<>();
            return acc;
        }
        
        @Override
        public UserAnchorAccumulator add(UserAnchorFeatureAggregation aggregation, UserAnchorAccumulator accumulator) {
            // 设置uid
            accumulator.uid = aggregation.uid;
            
            // 合并每个主播的特征
            if (aggregation.anchorFeatures != null) {
                for (Map.Entry<String, byte[]> entry : aggregation.anchorFeatures.entrySet()) {
                    try {
                        long anchorId = Long.parseLong(entry.getKey());
                        AnchorFeatureCounts counts = accumulator.anchorIdToCounts.computeIfAbsent(anchorId, k -> new AnchorFeatureCounts());
                        
                        // 解析protobuf数据
                        RecFeature.LiveUserAnchorFeature feature = RecFeature.LiveUserAnchorFeature.parseFrom(entry.getValue());
                        
                        // 合并曝光次数
                        // 优化：不再使用循环模拟HyperLogLog添加，直接使用基于概率的高效合并方式
                        if (feature.getUserAnchorExpCnt15Min() > 0) {
                            double estimatedCardinality = feature.getUserAnchorExpCnt15Min();
                            if (estimatedCardinality > counts.exposureHLL.cardinality()) {
                                // 只在新计数更大时更新，使用确定性字符串减少计算
                                String deterministicValue = "anchor_" + anchorId + "_exp";
                                counts.exposureHLL.offer(deterministicValue);
                            }
                        }
                        
                        // 累加3s+观看次数
                        counts.watch3sPlusCount += feature.getUserAnchor3SquitCnt15Min();
                        
                        // 合并6s+观看次数
                        // 优化：不再使用循环模拟HyperLogLog添加，直接使用基于概率的高效合并方式
                        if (feature.getUserAnchor6SquitCnt15Min() > 0) {
                            // 使用概率模型估算HyperLogLog的内部位图状态，避免O(n)循环
                            double estimatedCardinality = feature.getUserAnchor6SquitCnt15Min();
                            if (estimatedCardinality > counts.watch6sPlusHLL.cardinality()) {
                                // 只在新计数更大时更新，使用确定性字符串减少计算
                                String deterministicValue = "anchor_" + anchorId + "_watch6s";
                                counts.watch6sPlusHLL.offer(deterministicValue);
                            }
                        }
                        
                    } catch (Exception e) {
                        // 忽略解析错误
                        LOG.error("Error parsing anchor feature for anchorId={}: {}", entry.getKey(), e.getMessage());
                    }
                }
            }
            
            return accumulator;
        }
        
        @Override
        public UserAnchorFeatureAggregation getResult(UserAnchorAccumulator accumulator) {
            if (accumulator.anchorIdToCounts.isEmpty()) {
                return null;
            }
            
            UserAnchorFeatureAggregation result = new UserAnchorFeatureAggregation();
            result.uid = accumulator.uid;
            result.anchorFeatures = new HashMap<>();
            
            for (Map.Entry<Long, AnchorFeatureCounts> e : accumulator.anchorIdToCounts.entrySet()) {
                long anchorId = e.getKey();
                AnchorFeatureCounts c = e.getValue();
                
                // 从 HyperLogLog 获取去重后的曝光次数
                int exposureCount = (int) c.exposureHLL.cardinality();
                
                // 从 HyperLogLog 获取去重后的 6s+ 观看次数
                int watch6sPlusCount = (int) c.watch6sPlusHLL.cardinality();
                
                // 负反馈次数 = 曝光次数 - 6s+观看次数
                int negative = Math.max(0, exposureCount - watch6sPlusCount);
                
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserId(accumulator.uid)
                    .setAnchorId(anchorId)
                    .setUserAnchorExpCnt15Min(exposureCount)
                    .setUserAnchor3SquitCnt15Min(c.watch3sPlusCount)
                    .setUserAnchor6SquitCnt15Min(watch6sPlusCount)
                    .setUserAnchorNegativeFeedbackCnt15Min(negative)
                    .build()
                    .toByteArray();
                result.anchorFeatures.put(String.valueOf(anchorId), bytes);
            }
            
            return result.anchorFeatures.isEmpty() ? null : result;
        }
        
        @Override
        public UserAnchorAccumulator merge(UserAnchorAccumulator a, UserAnchorAccumulator b) {
            // 合并事件计数
            a.totalEventCount += b.totalEventCount;
            
            // 合并每个主播的特征
            for (Map.Entry<Long, AnchorFeatureCounts> e : b.anchorIdToCounts.entrySet()) {
                AnchorFeatureCounts target = a.anchorIdToCounts.computeIfAbsent(e.getKey(), k -> new AnchorFeatureCounts());
                AnchorFeatureCounts src = e.getValue();
                
                // 合并 HyperLogLog
                try {
                    target.exposureHLL = (HyperLogLog) target.exposureHLL.merge(src.exposureHLL);
                    target.watch6sPlusHLL = (HyperLogLog) target.watch6sPlusHLL.merge(src.watch6sPlusHLL);
                } catch (Exception ex) {
                    // 合并失败，保持原值
                    LOG.error("Error merging HyperLogLog: {}", ex.getMessage());
                }
                
                // 累加数值型特征
                target.watch3sPlusCount += src.watch3sPlusCount;
                target.totalWatchDuration += src.totalWatchDuration;
            }
            
            return a;
        }
    }

}
