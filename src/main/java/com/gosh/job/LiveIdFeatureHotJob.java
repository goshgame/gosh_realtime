package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.feature.RecFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.FlinkMonitorUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 直播间曝光特征任务：
 *  - 数据源：Kafka topic=event_live, event_type=1
 *  - 处理事件：live_exposure
 *  - 输出特征：按 live_id 聚合最近5分钟曝光次数
 *  - 时间窗口：5min，滑动间隔30秒
 *  - Redis Key：rec:live_id_feature:{live_id}:live5min
 */
public class LiveIdFeatureHotJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveIdFeatureHotJob.class);
    private static final String PREFIX = "rec:live_id_feature:{";
    private static final String SUFFIX = "}:live5min";
    private static final String EVENT_LIVE_EXPOSURE = "live_exposure";

    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Starting LiveIdFeatureHotJob - live_id exposure");
        LOG.info("Processing event_type=1 for live_exposure events");
        LOG.info("Window: 5min, slide: 10s, Redis key prefix: {}", PREFIX);
        LOG.info("========================================");

        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        LOG.info("Flink environment created");

        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "event_live"
        );
        LOG.info("Kafka source created for topic: event_live");

        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        LOG.info("Kafka data stream created");

        SingleOutputStreamOperator<LiveExposureEvent> exposureEventStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)")
            .flatMap(new LiveExposureEventParser())
            .name("Parse Live Exposure Events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LiveExposureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, recordTimestamp) -> event.eventTime)
            );

        SingleOutputStreamOperator<LiveIdExposureAgg> aggregatedStream = exposureEventStream
            .keyBy(event -> event.liveId)
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofSeconds(10)))
            .aggregate(new LiveIdExposureAggregator())
            .name("LiveId Exposure Aggregation 5min");

        SingleOutputStreamOperator<Tuple2<String, byte[]>> redisStream = aggregatedStream
            .filter(agg -> agg != null && agg.liveId > 0)
            .map(new MapFunction<LiveIdExposureAgg, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(LiveIdExposureAgg agg) throws Exception {
                    String redisKey = PREFIX + agg.liveId + SUFFIX;
                    byte[] bytes = buildExposureProtobuf(agg);
                    return new Tuple2<>(redisKey, bytes);
                }
            })
            .name("Convert to LiveId Protobuf");

        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(600);
        redisConfig.setCommand("SET");
        LOG.info("Redis config: TTL={}, Command={}", redisConfig.getTtl(), redisConfig.getCommand());

        RedisUtil.addRedisSink(
            redisStream,
            redisConfig,
            true,
            200
        );

        LOG.info("Job configured, starting execution...");
        String JOB_NAME = "Live Id Exposure Feature 5min Job";
        FlinkMonitorUtil.executeWithMonitor(env, JOB_NAME);
    }

    private static byte[] buildExposureProtobuf(LiveIdExposureAgg agg) {
        return RecFeature.RecLiveIdFeature.newBuilder()
            .setLiveId(agg.liveId)
            .setLiveIdExpCnt5Min(agg.exposureCount)
            .build()
            .toByteArray();
    }

    /**
     * live_exposure事件解析器
     */
    public static class LiveExposureEventParser implements FlatMapFunction<String, LiveExposureEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String value, Collector<LiveExposureEvent> out) throws Exception {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }
                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();
                if (!EVENT_LIVE_EXPOSURE.equals(event)) {
                    return;
                }
                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equals(eventData)) {
                    return;
                }
                JsonNode data = OBJECT_MAPPER.readTree(eventData);
                long liveId = data.path("live_id").asLong(0);
                long anchorId = data.path("anchor_id").asLong(0);
                if (liveId <= 0 || anchorId <= 0) {
                    return;
                }
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                }

                LiveExposureEvent evt = new LiveExposureEvent();
                evt.liveId = liveId;
                evt.anchorId = anchorId;
                evt.eventTime = eventTimeMillis;
                out.collect(evt);
            } catch (Exception e) {
                // 静默处理异常
            }
        }
    }

    /**
     * live_id曝光聚合器
     */
    public static class LiveIdExposureAggregator implements AggregateFunction<LiveExposureEvent, ExposureAccumulator, LiveIdExposureAgg> {
        @Override
        public ExposureAccumulator createAccumulator() {
            return new ExposureAccumulator();
        }

        @Override
        public ExposureAccumulator add(LiveExposureEvent event, ExposureAccumulator acc) {
            acc.liveId = event.liveId;
            acc.exposureCount++;
            return acc;
        }

        @Override
        public LiveIdExposureAgg getResult(ExposureAccumulator acc) {
            if (acc.liveId <= 0) {
                return null;
            }
            LiveIdExposureAgg result = new LiveIdExposureAgg();
            result.liveId = acc.liveId;
            result.exposureCount = acc.exposureCount;
            return result;
        }

        @Override
        public ExposureAccumulator merge(ExposureAccumulator a, ExposureAccumulator b) {
            if (a.liveId <= 0) {
                a.liveId = b.liveId;
            }
            a.exposureCount += b.exposureCount;
            return a;
        }
    }

    public static class ExposureAccumulator {
        public long liveId;
        public int exposureCount;
    }

    public static class LiveExposureEvent {
        public long liveId;
        public long anchorId;
        public long eventTime;
    }

    public static class LiveIdExposureAgg {
        public long liveId;
        public int exposureCount;
    }
}
