package com.gosh.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeatureDemoOuterClass;
import com.gosh.entity.UserLiveEvent;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DemoJob {
    private static final Logger LOG = LoggerFactory.getLogger(DemoJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:user_feature:";

    public static void main(String[] args) throws Exception {
        //第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        //第二步：创建Soure，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(KafkaEnvUtil.loadProperties(), "prod_parsed_events_with_watermark");

        //第三步：使用KafkaSource创建DataStream
        DataStreamSource kafkaSource =
                env.fromSource(
                        inputTopic,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "Kafka Source"
                );

        //todo
        // 3.1 解析源 JSON 数据
        //    使用flatMap将JSON字符串转换为UserLiveEvent对象
        //    设置并行度
        //    添加事件时间和水印策略
        SingleOutputStreamOperator parsedStream = kafkaSource
                .flatMap(new JsonParserFlatMap())
                .name("JSON Parser")
                .setParallelism(4)
                // 添加事件时间和水印策略
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserLiveEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> {
                                    // 将 event_time 字符串转换为毫秒时间戳
                                    try {
                                        // 假设 event_time 格式为 "yyyy-MM-dd'T'HH:mm:ss.SSS"（如示例中的 "2025-09-02T07:25:16.347"）
                                        LocalDateTime dateTime = LocalDateTime.parse(
                                                event.getEventTime(),
                                                DateTimeFormatter.ISO_LOCAL_DATE_TIME
                                        );
                                        long timestamp = dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
                                        // 打印解析后的时间戳，验证是否正确
                                        LOG.info("解析事件时间: {} -> {}", event.getEventTime(), timestamp);

                                        return timestamp;
                                    } catch (Exception e) {
                                        LOG.error("解析事件时间失败: {}", event.getEventTime(), e);
                                        return System.currentTimeMillis();
                                    }
                                })
                );
        ;

        // 3.2 根据 uid 分组，并使用滑动窗口进行聚合
        //   窗口大小1小时，滑动间隔10分钟
        //   UserLiveAggregator 实现聚合逻辑
        DataStream<UserLiveAggregation> aggregatedStream = parsedStream
                .keyBy(new KeySelector<UserLiveEvent, String>() {
                    @Override
                    public String getKey(UserLiveEvent value) throws Exception {
                        return value.getUid();
                    }
                })
                .windowAll(SlidingProcessingTimeWindows.of(Time.hours(1).toDuration(),Time.minutes(10).toDuration()))
                .aggregate(new UserLiveAggregator())
                .name("User Live Aggregation")
                ;
        //答应聚合后的结果，确认是否生成
        aggregatedStream.print();

        // 第四步：DataStream<T> aggregatedStream 转换到 protobuf
        // 4.1 定义 MapFunction 将 UserLiveAggregation 转换为 RecFeature Protobuf 对象
        DataStream<RecFeatureDemoOuterClass.RecFeature> protoStream =
                aggregatedStream.map(new AggregationToProtoMapper()).name("Aggregation to Protobuf");
        // 4.2 将 Protobuf 对象序列化为字节数组 DataStream<byte[]>
//        DataStream<Tuple2<String, String>> dataStream = tupleStream
//                .map(protoFeature -> protoFeature.toByteArray())
//                .name("Protobuf to Byte Array");

        // 4.3 将字节数组反序列化为RecFeature，再转换为Tuple2<String, String>
        DataStream<Tuple2<String, byte[]>> dataStream = protoStream
                .map(byteArray -> {
                    return new Tuple2<>(byteArray.getKey(), byteArray.getResutls().toString().getBytes());
                })
                .name("Byte Array to Tuple2");

//        // 4.3 定义protobuf解析器和key提取逻辑
//        Class<RecFeatureDemoOuterClass.RecFeature> protoClass = RecFeatureDemoOuterClass.RecFeature.class;
//        // 确保keyExtractor是可序列化的（显式类或Flink的Function）
//        Function<RecFeatureDemoOuterClass.RecFeature, String> keyExtractor = new UserKeyExtractor();


        // 第五步：创建sink，Redis环境
        // 5.1 加载Redis配置
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        // 5.2 添加Redis Sink
        //    支持异步和批量写入
        RedisUtil.addRedisSink(
                dataStream,
                redisConfig,
                false, // 异步写入
                100
        );
        //执行任务
        env.execute("Flink Demo Job");
    }

    /**
     * JSON 解析器 - 将 JSON 字符串解析为 UserLiveEvent 对象
     */
    public static class JsonParserFlatMap implements FlatMapFunction<String, UserLiveEvent> {
        @Override
        public void flatMap(String value, Collector<UserLiveEvent> out) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);

                String uid = jsonNode.has("uid") ? jsonNode.get("uid").asText() : null;
                String liveId = jsonNode.has("live_id") ? jsonNode.get("live_id").asText() : null;
                long watchTime = jsonNode.has("watch_time") ? jsonNode.get("watch_time").asLong() : 0;
                String rawEventTime = jsonNode.has("raw_event_time") ? jsonNode.get("raw_event_time").asText() : null;
                String eventTime = jsonNode.has("event_time") ? jsonNode.get("event_time").asText() : null;

                if (uid != null && liveId != null) {
                    UserLiveEvent event = new UserLiveEvent(uid, liveId, watchTime, rawEventTime, eventTime);
                    System.out.println(event);
                    out.collect(event);
                } else {
                    LOG.warn("Invalid JSON data: missing uid or live_id - {}", value);
                }
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", value, e);
            }
        }
    }

    /**
     * 用户直播事件聚合器
     */
    public static class UserLiveAggregator implements AggregateFunction<UserLiveEvent, UserLiveAccumulator, UserLiveAggregation> {
        @Override
        public UserLiveAccumulator createAccumulator() {
            return new UserLiveAccumulator();
        }

        @Override
        public UserLiveAccumulator add(UserLiveEvent event, UserLiveAccumulator accumulator) {
            LOG.info("Adding event to accumulator: {}{}", event);
            accumulator.uid = event.getUid();
            accumulator.liveIds.add(event.getLiveId());
            accumulator.totalWatchTime += event.getWatchTime();
            accumulator.eventCount++;

            // 更新时间范围
            if (accumulator.firstEventTime == null ||
                    event.getEventTime().compareTo(accumulator.firstEventTime) < 0) {
                accumulator.firstEventTime = event.getEventTime();
            }
            if (accumulator.lastEventTime == null ||
                    event.getEventTime().compareTo(accumulator.lastEventTime) > 0) {
                accumulator.lastEventTime = event.getEventTime();
            }
            LOG.info("聚合后结果：{}",accumulator);
            return accumulator;
        }

        @Override
        public UserLiveAggregation getResult(UserLiveAccumulator accumulator) {
            UserLiveAggregation result = new UserLiveAggregation(
                    accumulator.uid,
                    accumulator.liveIds,
                    accumulator.totalWatchTime,
                    accumulator.eventCount,
                    accumulator.firstEventTime,
                    accumulator.lastEventTime
            );
            // 打印聚合结果，确认是否生成
            System.out.println("Aggregated Result: " + result);
            return result;
        }

        @Override
        public UserLiveAccumulator merge(UserLiveAccumulator a, UserLiveAccumulator b) {
            a.liveIds.addAll(b.liveIds);
            a.totalWatchTime += b.totalWatchTime;
            a.eventCount += b.eventCount;

            if (a.firstEventTime == null ||
                    (b.firstEventTime != null && b.firstEventTime.compareTo(a.firstEventTime) < 0)) {
                a.firstEventTime = b.firstEventTime;
            }
            if (a.lastEventTime == null ||
                    (b.lastEventTime != null && b.lastEventTime.compareTo(a.lastEventTime) > 0)) {
                a.lastEventTime = b.lastEventTime;
            }

            return a;
        }
    }


    /**
     * 用户直播事件累加器
     */
    public static class UserLiveAccumulator {
        public String uid;
        public Set<String> liveIds = new HashSet<>();
        public long totalWatchTime = 0;
        public int eventCount = 0;
        public String firstEventTime;
        public String lastEventTime;

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public Set<String> getLiveIds() {
            return liveIds;
        }

        public void setLiveIds(Set<String> liveIds) {
            this.liveIds = liveIds;
        }

        public long getTotalWatchTime() {
            return totalWatchTime;
        }

        public void setTotalWatchTime(long totalWatchTime) {
            this.totalWatchTime = totalWatchTime;
        }

        public int getEventCount() {
            return eventCount;
        }

        public void setEventCount(int eventCount) {
            this.eventCount = eventCount;
        }

        public String getFirstEventTime() {
            return firstEventTime;
        }

        public void setFirstEventTime(String firstEventTime) {
            this.firstEventTime = firstEventTime;
        }

        public String getLastEventTime() {
            return lastEventTime;
        }

        public void setLastEventTime(String lastEventTime) {
            this.lastEventTime = lastEventTime;
        }

        @Override
        public String toString() {
            return "UserLiveAccumulator{" +
                    "uid='" + uid + '\'' +
                    ", liveIds=" + liveIds +
                    ", totalWatchTime=" + totalWatchTime +
                    ", eventCount=" + eventCount +
                    ", firstEventTime='" + firstEventTime + '\'' +
                    ", lastEventTime='" + lastEventTime + '\'' +
                    '}';
        }
    }

    /**
     * 用户直播聚合结果
     */
    public static class UserLiveAggregation {
        private String uid;
        private Set<String> liveIds;
        private long totalWatchTime;
        private int eventCount;
        private String firstEventTime;
        private String lastEventTime;

        public UserLiveAggregation() {
        }

        public UserLiveAggregation(String uid, Set<String> liveIds, long totalWatchTime,
                                   int eventCount, String firstEventTime, String lastEventTime) {
            this.uid = uid;
            this.liveIds = liveIds;
            this.totalWatchTime = totalWatchTime;
            this.eventCount = eventCount;
            this.firstEventTime = firstEventTime;
            this.lastEventTime = lastEventTime;
        }

        // Getter 和 Setter 方法
        public String getUid() { return uid; }
        public void setUid(String uid) { this.uid = uid; }
        public Set<String> getLiveIds() { return liveIds; }
        public void setLiveIds(Set<String> liveIds) { this.liveIds = liveIds; }
        public long getTotalWatchTime() { return totalWatchTime; }
        public void setTotalWatchTime(long totalWatchTime) { this.totalWatchTime = totalWatchTime; }
        public int getEventCount() { return eventCount; }
        public void setEventCount(int eventCount) { this.eventCount = eventCount; }
        public String getFirstEventTime() { return firstEventTime; }
        public void setFirstEventTime(String firstEventTime) { this.firstEventTime = firstEventTime; }
        public String getLastEventTime() { return lastEventTime; }
        public void setLastEventTime(String lastEventTime) { this.lastEventTime = lastEventTime; }

        @Override
        public String toString() {
            return "UserLiveAggregation{" +
                    "uid='" + uid + '\'' +
                    ", liveIds=" + liveIds +
                    ", totalWatchTime=" + totalWatchTime +
                    ", eventCount=" + eventCount +
                    ", firstEventTime='" + firstEventTime + '\'' +
                    ", lastEventTime='" + lastEventTime + '\'' +
                    '}';
        }
    }

    /**
     * 设置 key值
     */
    private static class UserKeyExtractor implements Function<RecFeatureDemoOuterClass.RecFeature, String>, Serializable {
        @Override
        public String apply(RecFeatureDemoOuterClass.RecFeature feature) {
            return PREFIX + feature.getKey();
        }
    }

    /**
     * 将 UserLiveAggregation 转换为 Protobuf 对象 RecUserFeatureOuterClass.RecUserFeature
     */
    public static class AggregationToProtoMapper implements MapFunction<UserLiveAggregation, RecFeatureDemoOuterClass.RecFeature> {
        private  final Logger LOG = LoggerFactory.getLogger(AggregationToProtoMapper.class);
        // 复用全局 ObjectMapper 避免重复创建
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public RecFeatureDemoOuterClass.RecFeature map(DemoJob.UserLiveAggregation aggregation) throws Exception {
            try {
                // 1. 构建结果映射（参考 RecUserFeatureSinkJob 中 resutls 的 JSON 格式）
                Map<String, Object> resultMap = new HashMap<>();
                // 事件总数（对应示例中的 user_foru_explive_cnt_24h）
                resultMap.put("user_foru_explive_cnt_24h", aggregation.getEventCount());
                // 观看的直播数量（对应示例中的 user_foru_joinlive_cnt_24h）
                resultMap.put("user_foru_joinlive_cnt_24h", aggregation.getLiveIds().size());
                // 总观看时长
                resultMap.put("user_total_watch_time_24h", aggregation.getTotalWatchTime());

                // 2. 将映射转换为 JSON 字符串（作为 resutls 字段）
                String resultsJson = objectMapper.writeValueAsString(resultMap);

                // 3. 构建 Protobuf 对象并设置字段
                return RecFeatureDemoOuterClass.RecFeature.newBuilder()
                        .setKey(aggregation.getUid()) // key 字段设为用户 ID
                        .setResutls(resultsJson)      // resutls 字段设为聚合统计的 JSON
                        .build();

            } catch (JsonProcessingException e) {
                LOG.error("转换 UserLiveAggregation 到 Protobuf 失败: {}", aggregation, e);
                throw new RuntimeException("聚合结果转换 Protobuf 失败", e);
            }
        }
    }

}
