package com.gosh.job;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.*;
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

import java.util.HashMap;
import java.util.Map;

public class LiveUserAnchorFeature15minJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserAnchorFeature15minJob.class);
    private static final String PREFIX = "rec:user_anchor_feature:{";
    private static final String SUFFIX = "}:live15min";
    private static final int HLL_LOG2M = 14; // HyperLogLog精度参数
    
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

        // 第二步：创建Source，Kafka环境（topic=advertise）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "advertise"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)")
            .process(new org.apache.flink.streaming.api.functions.ProcessFunction<String, String>() {
                private transient long totalCount = 0;
                private transient long exposureMatchCount = 0;
                private transient long viewMatchCount = 0;
                private transient long enterMatchCount = 0;
                private transient long exitMatchCount = 0;
                
                @Override
                public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                    totalCount++;
                    
                    // 字符串匹配各种事件
                    if (value.contains("live_exposure")) {
                        exposureMatchCount++;
                        if (exposureMatchCount <= 3) {
                            LOG.info("[String Match Exposure {}/3] Full data: {}", exposureMatchCount, value);
                        }
                    }
                    if (value.contains("live_view")) {
                        viewMatchCount++;
                        if (viewMatchCount <= 3) {
                            LOG.info("[String Match View {}/3] Full data: {}", viewMatchCount, value);
                        }
                    }
                    if (value.contains("enter_liveroom")) {
                        enterMatchCount++;
                        if (enterMatchCount <= 3) {
                            LOG.info("[String Match Enter {}/3] Full data: {}", enterMatchCount, value);
                        }
                    }
                    if (value.contains("exit_liveroom")) {
                        exitMatchCount++;
                        if (exitMatchCount <= 3) {
                            LOG.info("[String Match Exit {}/3] Full data: {}", exitMatchCount, value);
                        }
                    }
                    
                    if (totalCount % 50000 == 0) {
                        LOG.info("[String Match Summary] Total={}, Exposure={}, View={}, Enter={}, Exit={}", 
                            totalCount, exposureMatchCount, viewMatchCount, enterMatchCount, exitMatchCount);
                    }
                    
                    out.collect(value);
                }
            })
            .name("Event String Matcher");

        // 3.1 解析 user_event_log 为 Live 用户-主播 事件
        SingleOutputStreamOperator<LiveUserAnchorEvent> eventStream = filteredStream
            .flatMap(new LiveEventParser())
            .name("Parse Live User-Anchor Events");

        // 第四步：按 uid 分组并进行 15 分钟窗口（滑动 5 秒钟）聚合
        DataStream<UserAnchorFeatureAggregation> aggregatedStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
                @Override
                public Long getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.uid;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(15),
                Time.seconds(5)
            ))
            .aggregate(new UserAnchorFeatureAggregator())
            .name("User-Anchor Feature Aggregation");

        // 第五步：转换为Protobuf并写入Redis（HashMap：key=uid，field=anchorId）
        DataStream<Tuple2<String, Map<String, byte[]>>> dataStream = aggregatedStream
            .filter(agg -> agg != null && agg.anchorFeatures != null && !agg.anchorFeatures.isEmpty())
            .map(new MapFunction<UserAnchorFeatureAggregation, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(UserAnchorFeatureAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    return new Tuple2<>(redisKey, agg.anchorFeatures);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 添加Redis写入前采样日志
        dataStream
            .process(new org.apache.flink.streaming.api.functions.ProcessFunction<Tuple2<String, Map<String, byte[]>>, Tuple2<String, Map<String, byte[]>>>() {
                private transient int sampleCount = 0;
                private transient long lastLogTime = 0;
                
                @Override
                public void processElement(Tuple2<String, Map<String, byte[]>> value, Context ctx, Collector<Tuple2<String, Map<String, byte[]>>> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastLogTime > 60000) { // 每60秒采样一次
                        lastLogTime = now;
                        if (sampleCount < 3) {
                            sampleCount++;
                            LOG.info("[Redis Sample {}/3] About to write: key={}, fieldCount={}", 
                                sampleCount, value.f0, value.f1 != null ? value.f1.size() : 0);
                            
                            // 打印第一个field的protobuf解析结果
                            if (value.f1 != null && !value.f1.isEmpty()) {
                                Map.Entry<String, byte[]> firstEntry = value.f1.entrySet().iterator().next();
                                try {
                                    RecFeature.LiveUserAnchorFeature feature = RecFeature.LiveUserAnchorFeature.parseFrom(firstEntry.getValue());
                                    LOG.info("[Redis Value Sample {}/3] anchorId={}, userId={}, expCnt={}, 3sQuit={}, 6sQuit={}, negative={}", 
                                        sampleCount, firstEntry.getKey(), feature.getUserId(), 
                                        feature.getUserAnchorExpCnt15Min(), feature.getUserAnchor3SquitCnt15Min(), 
                                        feature.getUserAnchor6SquitCnt15Min(), feature.getUserAnchorNegativeFeedbackCnt15Min());
                                } catch (Exception e) {
                                    LOG.warn("[Redis Value Sample {}/3] Failed to parse protobuf: {}", sampleCount, e.getMessage());
                                }
                            }
                        }
                    }
                    out.collect(value);
                }
            })
            .name("Redis Write Sampling");

        // 第六步：创建sink，Redis环境（TTL=10分钟，DEL_HMSET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(3600 * 72);
        redisConfig.setCommand("DEL_HMSET");
        LOG.info("Redis config: TTL={}, Command={}", 
            redisConfig.getTtl(), redisConfig.getCommand());
        
        RedisUtil.addRedisHashMapSink(
            dataStream,
            redisConfig,
            true,
            100
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
            acc.uid = event.uid;
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
}
