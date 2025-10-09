package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
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
    private static String PREFIX = "rec:user_anchor_feature:{";
    private static String SUFFIX = "}:live15min";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting LiveUserAnchorFeature15minJob - looking for advertise topic with event_type=3");
        
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

        // 3.0 预过滤 - 只保留 event_type=3 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(3))
            .name("Pre-filter Advertise Events");

        // 3.1 解析 recommend_data.event_data（JSON 字符串）为 Live 用户-主播 事件
        SingleOutputStreamOperator<LiveUserAnchorEvent> eventStream = filteredStream
            .flatMap(new LiveEventParser())
            .name("Parse Live Quit Events");

        // 第四步：按 uid 分组并进行 15 分钟窗口（滑动 5 分钟）聚合
        DataStream<UserAnchorFeatureAggregation> aggregatedStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
                @Override
                public Long getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.uid;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(15),
                Time.seconds(15)
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
                        }
                    }
                    out.collect(value);
                }
            })
            .name("Redis Write Sampling");

        // 第六步：创建sink，Redis环境（TTL=10分钟，DEL_HMSET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(300);
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
        env.execute("Live User-Anchor Feature 15min Job");
    }

    // 输入事件：从 advertise.recommend_data.event_data 解析
    public static class LiveUserAnchorEvent {
        public long uid;
        public long anchorId;
        public long watchTime;
    }

        // 解析器：解析外层 JSON，提取 recommend_data.event_data 再解析内层 JSON
    public static class LiveEventParser implements FlatMapFunction<String, LiveUserAnchorEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static volatile long totalParsedEvents = 0;

        @Override
        public void flatMap(String value, Collector<LiveUserAnchorEvent> out) throws Exception {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                JsonNode recommend = root.path("recommend_data");
                if (recommend.isMissingNode()) {
                    return;
                }
                JsonNode eventDataNode = recommend.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty()) {
                    return;
                }
                JsonNode liveQuit = OBJECT_MAPPER.readTree(eventData);
                long uid = liveQuit.path("uid").asLong(0);
                long anchorId = liveQuit.path("anchor_id").asLong(0);
                long watchTime = liveQuit.path("watch_time").asLong(0);
                if (uid <= 0 || anchorId <= 0) {
                    return;
                }
                LiveUserAnchorEvent evt = new LiveUserAnchorEvent();
                evt.uid = uid;
                evt.anchorId = anchorId;
                evt.watchTime = watchTime;
                totalParsedEvents++;
                if (totalParsedEvents % 100 == 0) {
                    LOG.info("Total parsed events so far: {}", totalParsedEvents);
                }
                out.collect(evt);
            } catch (Exception e) {
                LOG.warn("Failed to parse advertise event: {}", value, e);
            }
        }
    }

    // 聚合器：统计每个 uid 对每个 anchor 的曝光、3s+进房、6s+进房与负反馈（派生）
    public static class UserAnchorFeatureAggregator implements AggregateFunction<LiveUserAnchorEvent, UserAnchorAccumulator, UserAnchorFeatureAggregation> {
        @Override
        public UserAnchorAccumulator createAccumulator() {
            return new UserAnchorAccumulator();
        }

        @Override
        public UserAnchorAccumulator add(LiveUserAnchorEvent event, UserAnchorAccumulator acc) {
            acc.uid = event.uid;
            Counts counts = acc.anchorIdToCounts.computeIfAbsent(event.anchorId, k -> new Counts());
            counts.exposureCount += 1;
            if (event.watchTime >= 3) counts.watch3sPlusCount += 1;
            if (event.watchTime >= 6) counts.watch6sPlusCount += 1;
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

            for (Map.Entry<Long, Counts> e : acc.anchorIdToCounts.entrySet()) {
                long anchorId = e.getKey();
                Counts c = e.getValue();
                int negative = Math.max(0, c.exposureCount - c.watch6sPlusCount);
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserId(acc.uid)
                    .setAnchorId(anchorId)
                    .setUserAnchorExpCnt15Min(c.exposureCount)
                    .setUserAnchor3SquitCnt15Min(c.watch3sPlusCount)
                    .setUserAnchor6SquitCnt15Min(c.watch6sPlusCount)
                    .setUserAnchorNegativeFeedbackCnt15Min(negative)
                    .build()
                    .toByteArray();
                result.anchorFeatures.put(String.valueOf(anchorId), bytes);
            }
            return result.anchorFeatures.isEmpty() ? null : result;
        }

        @Override
        public UserAnchorAccumulator merge(UserAnchorAccumulator a, UserAnchorAccumulator b) {
            UserAnchorAccumulator merged = a;
            for (Map.Entry<Long, Counts> e : b.anchorIdToCounts.entrySet()) {
                Counts target = merged.anchorIdToCounts.computeIfAbsent(e.getKey(), k -> new Counts());
                Counts src = e.getValue();
                target.exposureCount += src.exposureCount;
                target.watch3sPlusCount += src.watch3sPlusCount;
                target.watch6sPlusCount += src.watch6sPlusCount;
            }
            return merged;
        }
    }

    public static class UserAnchorAccumulator {
        public long uid;
        public Map<Long, Counts> anchorIdToCounts = new HashMap<>();
    }

    public static class Counts {
        public int exposureCount;
        public int watch3sPlusCount;
        public int watch6sPlusCount;
    }

    public static class UserAnchorFeatureAggregation {
        public long uid;
        public Map<String, byte[]> anchorFeatures; // anchorId -> protobuf bytes
    }
}