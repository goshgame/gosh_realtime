package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.entity.RecUserFeatureOuterClass;
import com.gosh.entity.UserLiveEvent;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import com.gosh.entity.RecUserFeatureOuterClass.RecUserFeature;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DemoProtoJob {
    private static final Logger LOG = LoggerFactory.getLogger(DemoProtoJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        // 第二步：创建Kafka环境
        KafkaSource<String> kafkSource = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(),
                "prod_parsed_events_with_watermark"
        );

        // 第三步：创建Source，使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSourceStream =
                env.fromSource(
                        kafkSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "Kafka Source"
                );

        // 第四步：解析原始数据，转换为UserLiveEvent对象，并提取事件时间
        SingleOutputStreamOperator<UserLiveEvent> parsedStream = kafkaSourceStream
                //解析kafka数据
                .flatMap(new JsonParserFlatMap())
                .name("JSON Parser")
                .setParallelism(4)
                // 添加事件时间和水印策略
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserLiveEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> {
                                    // 将event_time字符串转换为毫秒时间戳
                                    try {
                                        LocalDateTime dateTime = LocalDateTime.parse(
                                                event.getEventTime(),
                                                DateTimeFormatter.ISO_LOCAL_DATE_TIME
                                        );
                                        return dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
                                    } catch (Exception e) {
                                        LOG.error("解析事件时间失败: {}", event.getEventTime(), e);
                                        return System.currentTimeMillis();
                                    }
                                })
                );

        // 第五步：数据聚合；根据UID分组，在步长10分钟，24小时窗口内聚合计算用户特征
        DataStream<RecUserFeature> userFeatureStream = parsedStream
                //按uid分组
                .keyBy((KeySelector<UserLiveEvent, String>) UserLiveEvent::getUid)
                .windowAll(SlidingProcessingTimeWindows.of(Time.hours(1).toDuration(),Time.minutes(10).toDuration()))
                //聚合计算用户特征
                .aggregate(new UserFeatureAggregator())
                .name("User Feature Aggregation");

        // 打印输出结果
        userFeatureStream.print().name("Feature Output");
        SingleOutputStreamOperator<Tuple2<String, byte[]>> mapStream = userFeatureStream.map(recUserFeature -> {
            return new Tuple2<String, byte[]>(recUserFeature.getKey(), recUserFeature.getResutls().toString().getBytes());
        });

        //第六步：写入Sink
        // 这里可以添加将userFeatureStream写入Readis
        RedisUtil.addRedisSink(mapStream);

        // 执行任务
        env.execute("User Feature Calculation Job");
    }

    /**
     * JSON解析器 - 将JSON字符串解析为UserLiveEvent对象
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

                if (uid != null && liveId != null && eventTime != null) {
                    UserLiveEvent event = new UserLiveEvent(uid, liveId, watchTime, rawEventTime, eventTime);
                    out.collect(event);
                } else {
                    LOG.warn("无效JSON数据: 缺少uid/live_id/event_time - {}", value);
                }
            } catch (Exception e) {
                LOG.error("JSON解析失败: {}", value, e);
            }
        }
    }

    /**
     * 用户特征聚合器 - 计算并生成RecUserFeature对象
     */
    public static class UserFeatureAggregator implements AggregateFunction<
            UserLiveEvent,
            UserFeatureAccumulator,
            RecUserFeature> {

        @Override
        public UserFeatureAccumulator createAccumulator() {
            return new UserFeatureAccumulator();
        }

        @Override
        public UserFeatureAccumulator add(UserLiveEvent event, UserFeatureAccumulator accumulator) {
            // 初始化用户ID
            accumulator.uid = event.getUid();

            // 统计进房次数（live_id出现次数）
            accumulator.joinLiveCount++;

            // 维护最近3次事件（按event_time倒序）
            accumulator.recentEvents.add(event);
            // 按事件时间倒序排序
            accumulator.recentEvents.sort((e1, e2) -> e2.getEventTime().compareTo(e1.getEventTime()));
            // 只保留最近3条
            if (accumulator.recentEvents.size() > 3) {
                accumulator.recentEvents = accumulator.recentEvents.subList(0, 3);
            }

            return accumulator;
        }

        @Override
        public RecUserFeature getResult(UserFeatureAccumulator accumulator) {
            // 构建最近3次退房数据字符串
            StringBuilder quitLiveBuilder = new StringBuilder();
            for (int i = 0; i < accumulator.recentEvents.size(); i++) {
                UserLiveEvent event = accumulator.recentEvents.get(i);
                if (i > 0) {
                    quitLiveBuilder.append(" | ");
                }
                quitLiveBuilder.append(event.getLiveId()).append(":").append(event.getWatchTime());
            }

            // 构建RecUserFeature对象
            return RecUserFeature.newBuilder()
                    .setKey("rec:user_feature:12345")
                    .setResutls("{'user_foru_explive_cnt_24h':10,'user_foru_joinlive_cnt_24h':3,'user_quitlive_3latest':'live_678:300|live_910:150'}")
                    .build();
        }

        @Override
        public UserFeatureAccumulator merge(UserFeatureAccumulator a, UserFeatureAccumulator b) {
            // 合并进房次数
            a.joinLiveCount += b.joinLiveCount;

            // 合并最近事件列表
            List<UserLiveEvent> allEvents = new ArrayList<>(a.recentEvents);
            allEvents.addAll(b.recentEvents);
            // 去重并排序（按时间倒序）
            allEvents.sort((e1, e2) -> e2.getEventTime().compareTo(e1.getEventTime()));
            // 保留最近3条
            if (allEvents.size() > 3) {
                allEvents = allEvents.subList(0, 3);
            }
            a.recentEvents = allEvents;

            return a;
        }
    }

    /**
     * 特征计算累加器 - 存储中间计算结果
     */
    public static class UserFeatureAccumulator {
        public String uid;
        public int joinLiveCount = 0;  // 进房次数
        public List<UserLiveEvent> recentEvents = new ArrayList<>();  // 最近事件列表
    }
}