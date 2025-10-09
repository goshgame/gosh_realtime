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

public class LiveUserAnchorFeature3hJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserAnchorFeature3hJob.class);
    private static String PREFIX = "rec:user_anchor_feature:{";
    private static String SUFFIX = "}:live3h";

    public static void main(String[] args) throws Exception {
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

        // 第四步：按 uid 分组并进行 3 小时窗口（滑动 10 分钟）聚合
        DataStream<UserAnchorFeature3hAggregation> aggregatedStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, Long>() {
                @Override
                public Long getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.uid;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(3),
                Time.minutes(10)
            ))
            .aggregate(new UserAnchorFeature3hAggregator())
            .name("User-Anchor Feature 3h Aggregation");

        // 第五步：转换为Protobuf并写入Redis（HashMap：key=uid，field=anchorId）
        DataStream<Tuple2<String, Map<String, byte[]>>> dataStream = aggregatedStream
            .filter(agg -> agg != null && agg.anchorFeatures != null && !agg.anchorFeatures.isEmpty())
            .map(new MapFunction<UserAnchorFeature3hAggregation, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(UserAnchorFeature3hAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    return new Tuple2<>(redisKey, agg.anchorFeatures);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境（TTL=30分钟，DEL_HMSET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(1800);
        redisConfig.setCommand("DEL_HMSET");
        RedisUtil.addRedisHashMapSink(
            dataStream,
            redisConfig,
            true,
            100
        );

        env.execute("Live User-Anchor Feature 3h Job");
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
                out.collect(evt);
            } catch (Exception e) {
                LOG.warn("Failed to parse advertise event: {}", value, e);
            }
        }
    }

    // 聚合器：统计每个 uid 对每个 anchor 的曝光、3s+进房、6s+进房与负反馈（派生）
    public static class UserAnchorFeature3hAggregator implements AggregateFunction<LiveUserAnchorEvent, UserAnchorAccumulator, UserAnchorFeature3hAggregation> {
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
        public UserAnchorFeature3hAggregation getResult(UserAnchorAccumulator acc) {
            if (acc.anchorIdToCounts.isEmpty()) {
                return null;
            }
            UserAnchorFeature3hAggregation result = new UserAnchorFeature3hAggregation();
            result.uid = acc.uid;
            result.anchorFeatures = new HashMap<>();

            for (Map.Entry<Long, Counts> e : acc.anchorIdToCounts.entrySet()) {
                long anchorId = e.getKey();
                Counts c = e.getValue();
                int negative = Math.max(0, c.exposureCount - c.watch6sPlusCount);
                byte[] bytes = RecFeature.LiveUserAnchorFeature.newBuilder()
                    .setUserId(acc.uid)
                    .setAnchorId(anchorId)
                    .setUserAnchorExpCnt3H(c.exposureCount)
                    .setUserAnchor3SquitCnt1H(c.watch3sPlusCount)
                    .setUserAnchor6SquitCnt1H(c.watch6sPlusCount)
                    .setUserAnchorNegativeFeedbackCnt3H(negative)
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

    public static class UserAnchorFeature3hAggregation {
        public long uid;
        public Map<String, byte[]> anchorFeatures; // anchorId -> protobuf bytes
    }
}