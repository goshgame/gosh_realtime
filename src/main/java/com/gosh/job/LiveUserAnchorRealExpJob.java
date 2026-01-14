package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.feature.RecFeature;
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

public class LiveUserAnchorRealExpJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserAnchorRealExpJob.class);
    private static final String PREFIX = "rec:user_anchor_feature:{";
    private static final String SUFFIX = "}:livereal";
    // 每个窗口内每个用户的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 500;
    
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

        // 第二步：创建Source，Kafka环境（topic=event_live）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "event_live"
        );

        // 第三步：使用KafkaSource创建DataStream（使用 processing-time，不使用 watermarks）
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)");

        // 3.1 解析 user_event_log 为 Live 用户-主播 事件
        SingleOutputStreamOperator<LiveUserAnchorEvent> eventStream = filteredStream
            .flatMap(new LiveEventParser())
            .name("Parse Live User-Anchor Events");

        // 第四步：使用自定义 per-uid 窗口（最近接触主播队列），每次新事件到来即触发下游写 Redis（负反馈计数固定为 1）
        SingleOutputStreamOperator<Tuple2<String, Map<String, byte[]>>> dataStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
                @Override
                public Long getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.uid;
                }
            })
            .process(new UserRecentAnchorProcess())
            .name("User Recent Anchor Process");

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
        public long watchDuration;  // 观看时长（浏览和退房事件的stay_duration）
        public int scene;
        public long eventTime;  // 事件发生时间戳
    }

    // 解析器：解析 user_event_log.event 和 user_event_log.event_data
    public static class LiveEventParser implements FlatMapFunction<String, LiveUserAnchorEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        // private static volatile long totalParsedEvents = 0;
        // private static volatile long exposureCount = 0;
        // private static volatile long viewCount = 0;
        // private static volatile long enterCount = 0;
        // private static volatile long exitCount = 0;

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
                
                // 只处理2种事件
                if (!EVENT_LIVE_EXPOSURE.equals(event) && 
                    !EVENT_ENTER_LIVEROOM.equals(event) ) {
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
                // int scene = 0;
                // if (EVENT_ENTER_LIVEROOM.equals(event) || EVENT_EXIT_LIVEROOM.equals(event)) {
                //     scene = data.path("from_scene").asInt(0);  // 进房/退房用 from_scene
                // } else {
                //     scene = data.path("scene").asInt(0);        // 曝光/浏览用 scene
                // }
                
                if (uid <= 0 || anchorId <= 0) {
                    return;
                }
                
                // 过滤 scene：只保留 1, 3, 4, 5
                // if (scene != 1 && scene != 3 && scene != 4 && scene != 5) {
                //     return;
                // }
                
                LiveUserAnchorEvent evt = new LiveUserAnchorEvent();
                evt.uid = uid;
                evt.anchorId = anchorId;
                evt.eventType = event;
                
                // 从user_event_log中提取事件时间戳
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                } 
                evt.eventTime = eventTimeMillis;
                
                // 提取观看时长（浏览和退房事件）
                if (EVENT_LIVE_VIEW.equals(event)) {
                    evt.watchDuration = data.path("watch_duration").asLong(0);
                } else if (EVENT_EXIT_LIVEROOM.equals(event)) {
                    evt.watchDuration = data.path("stay_duration").asLong(0);
                }
                
                // 统计各事件类型数量并打印样例
                // totalParsedEvents++;
                // if (EVENT_LIVE_EXPOSURE.equals(event)) {
                //     exposureCount++;
                //     if (exposureCount <= 3) {
                //         LOG.info("[Parse Sample Exposure {}/3] uid={}, anchorId={}, scene={}", 
                //             exposureCount, uid, anchorId, scene);
                //     }
                // } else if (EVENT_LIVE_VIEW.equals(event)) {
                //     viewCount++;
                //     if (viewCount <= 3) {
                //         LOG.info("[Parse Sample View {}/3] uid={}, anchorId={}, scene={}, watchDuration={}", 
                //             viewCount, uid, anchorId, scene, evt.watchDuration);
                //     }
                // } else if (EVENT_ENTER_LIVEROOM.equals(event)) {
                //     enterCount++;
                //     if (enterCount <= 3) {
                //         LOG.info("[Parse Sample Enter {}/3] uid={}, anchorId={}, scene={}", 
                //             enterCount, uid, anchorId, scene);
                //     }
                // } else if (EVENT_EXIT_LIVEROOM.equals(event)) {
                //     exitCount++;
                //     if (exitCount <= 3) {
                //         LOG.info("[Parse Sample Exit {}/3] uid={}, anchorId={}, scene={}, watchDuration={}", 
                //             exitCount, uid, anchorId, scene, evt.watchDuration);
                //     }
                // }
                
                // if (totalParsedEvents % 10000 == 0) {
                //     LOG.info("[Parser Summary] Total={}, Exposure={}, View={}, Enter={}, Exit={}", 
                //         totalParsedEvents, exposureCount, viewCount, enterCount, exitCount);
                // }
                
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
            
            // 曝光次数：直接累加事件
            if (EVENT_LIVE_EXPOSURE.equals(event.eventType)) {
                counts.exposureCount++;
            }
            
            // 3s+观看次数：进房事件计数
            if (EVENT_ENTER_LIVEROOM.equals(event.eventType)) {
                counts.watch3sPlusCount += 1;
            }
            
            // 6s+观看次数：浏览/退房事件且时长>=6s
            if ((EVENT_LIVE_VIEW.equals(event.eventType) || EVENT_EXIT_LIVEROOM.equals(event.eventType)) 
                && event.watchDuration >= 6) {
                counts.watch6sPlusCount++;
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
                
                int exposureCount = c.exposureCount;
                int watch6sPlusCount = c.watch6sPlusCount;
                
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
                
                target.exposureCount += src.exposureCount;
                target.watch3sPlusCount += src.watch3sPlusCount;
                target.watch6sPlusCount += src.watch6sPlusCount;
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
        public int exposureCount = 0;               // 曝光次数
        public int watch3sPlusCount = 0;            // 3s+观看次数（进房）
        public int watch6sPlusCount = 0;            // 6s+观看次数
        public long totalWatchDuration = 0;         // 总观看时长
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
                        
                        counts.exposureCount += feature.getUserAnchorExpCnt15Min();
                        counts.watch3sPlusCount += feature.getUserAnchor3SquitCnt15Min();
                        counts.watch6sPlusCount += feature.getUserAnchor6SquitCnt15Min();
                        
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
                
                int exposureCount = c.exposureCount;
                int watch6sPlusCount = c.watch6sPlusCount;
                
                // 负反馈次数 = 曝光次数 - 6s+观看次数
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserId(accumulator.uid)
                    .setAnchorId(anchorId)
                    .setUserAnchorExpCnt15Min(exposureCount)
                    .setUserAnchor3SquitCnt15Min(c.watch3sPlusCount)
                    .setUserAnchor6SquitCnt15Min(watch6sPlusCount)
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
                
                target.exposureCount += src.exposureCount;
                target.watch3sPlusCount += src.watch3sPlusCount;
                target.watch6sPlusCount += src.watch6sPlusCount;
                target.totalWatchDuration += src.totalWatchDuration;
            }
            
            return a;
        }
    }
    /**
     * Per-uid recent anchor process: maintain a recent-anchor queue (exposure or enter events),
     * max size = 20, expire = 15 minutes. On each new event update, emit a Tuple2<redisKey, map<anchorId, bytes>>
     * where each anchor's protobuf has negative feedback count = 1.
     */
    public static class UserRecentAnchorProcess extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Long, LiveUserAnchorEvent, Tuple2<String, Map<String, byte[]>>> {
        private transient org.apache.flink.api.common.state.ListState<RecentAnchorEntry> anchorState;
        private final long expireMs = 15L * 60L * 1000L; // 15 minutes
        private final int maxSize = 20;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            anchorState = getRuntimeContext().getListState(
                new org.apache.flink.api.common.state.ListStateDescriptor<>("recentAnchors", RecentAnchorEntry.class)
            );
        }

        @Override
        public void processElement(LiveUserAnchorEvent value, Context ctx, Collector<Tuple2<String, Map<String, byte[]>>> out) throws Exception {
            if (value == null) return;

            // only treat exposure or enter events as "contact"
            if (!EVENT_LIVE_EXPOSURE.equals(value.eventType) && !EVENT_ENTER_LIVEROOM.equals(value.eventType)) {
                return;
            }

            long now = ctx.timerService().currentProcessingTime();

            java.util.LinkedHashMap<Long, RecentAnchorEntry> map = listStateToAnchorMap(anchorState, now);

            // remove duplicate if exists
            if (map.containsKey(value.anchorId)) {
                map.remove(value.anchorId);
            }

            // append to end
            RecentAnchorEntry entry = new RecentAnchorEntry();
            entry.anchorId = value.anchorId;
            entry.timestamp = now;
            map.put(value.anchorId, entry);

            // remove expired entries
            java.util.Iterator<java.util.Map.Entry<Long, RecentAnchorEntry>> itExp = map.entrySet().iterator();
            while (itExp.hasNext()) {
                java.util.Map.Entry<Long, RecentAnchorEntry> e = itExp.next();
                if (e.getValue().timestamp + expireMs < now) {
                    itExp.remove();
                }
            }

            // trim by size
            while (map.size() > maxSize) {
                java.util.Iterator<Long> it = map.keySet().iterator();
                if (it.hasNext()) {
                    it.next();
                    it.remove();
                } else {
                    break;
                }
            }

            // write back to state
            writeAnchorMapToListState(anchorState, map);

            // build protobuf bytes: negative feedback count = 1 for each anchor
            Map<String, byte[]> anchorFeatures = new HashMap<>();
            Long currentKey = ctx.getCurrentKey();
            if (currentKey == null) return;
            for (RecentAnchorEntry a : map.values()) {
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserAnchorNegativeFeedbackCnt15Min(1)
                    .build()
                    .toByteArray();
                anchorFeatures.put(String.valueOf(a.anchorId), bytes);
            }

            if (!anchorFeatures.isEmpty()) {
                String redisKey = PREFIX + currentKey + SUFFIX;
                out.collect(new Tuple2<>(redisKey, anchorFeatures));
            }
        }

        private java.util.LinkedHashMap<Long, RecentAnchorEntry> listStateToAnchorMap(org.apache.flink.api.common.state.ListState<RecentAnchorEntry> state, long now) throws Exception {
            java.util.LinkedHashMap<Long, RecentAnchorEntry> map = new java.util.LinkedHashMap<>();
            if (state == null) return map;
            for (RecentAnchorEntry e : state.get()) {
                if (e == null) continue;
                if (e.timestamp + expireMs < now) continue;
                map.put(e.anchorId, e);
            }
            return map;
        }

        private void writeAnchorMapToListState(org.apache.flink.api.common.state.ListState<RecentAnchorEntry> state, java.util.LinkedHashMap<Long, RecentAnchorEntry> map) throws Exception {
            state.clear();
            for (RecentAnchorEntry e : map.values()) {
                state.add(e);
            }
        }

        public static class RecentAnchorEntry {
            public long anchorId;
            public long timestamp;
        }
    }

}
