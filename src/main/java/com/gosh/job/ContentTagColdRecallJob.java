package com.gosh.job;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.http.impl.client.AIMDBackoffManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ContentTagColdRecallJob {
    private static final Logger LOG = LoggerFactory.getLogger(ContentTagColdRecallJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String PREFIX = "rec:post:{";
    private static final String SUFFIX = "}:tag_cold_start";
    private static final String kafkaTopic = "rec";
    private static final int keepEventType = 11;


    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
//        env.setParallelism(1);

        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), kafkaTopic
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
                inputTopic,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source"
        );

        // 3.0 预过滤 - 只保留我们需要的事件类型
        DataStream<String> filteredStream = kafkaSource
                .filter(EventFilterUtil.createFastEventTypeFilter(keepEventType))
                .name("Pre-filter Events");

        // 3.1 解析打标事件 (event_type=11)
        SingleOutputStreamOperator<AiTagParseCommon.PostTagEvent> postTagStream = filteredStream
                .flatMap(new AiTagParseCommon.PostTagEventParser())
                .name("Parse ai-tag Events");

        // 3.2 将打标事件转换为倒排索引事件
        DataStream<AiTagParseCommon.TagPostEvent> tagPostStream = postTagStream
                .flatMap(new AiTagParseCommon.PostToTagMapper())
                .name("Post-Tag to Tag-Posts Transform");

        // 3.2 获取redis数据，同时将打标事件解析到的数据进行合并，重新写入redis




        // 替换 ContentTagColdRecallJob.java 中的 3.2 步骤注释部分为以下代码:
        // 按标签分组并聚合
        DataStream<AiTagParseCommon.TagPostsAggregation> aggregatedStream = tagPostStream
                .keyBy(new KeySelector<AiTagParseCommon.TagPostEvent, String>() {
                    @Override
                    public String getKey(AiTagParseCommon.TagPostEvent value) throws Exception {
                        // 获取第一个标签作为key
                        if (value.tag != null && !value.tag.isEmpty()) {
                            return value.tag;
                        }
                        return "";
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)))
                .aggregate(new AiTagParseCommon.TagPostsAggregator())
                .name("Aggregate Posts by Tag");

        // 转换为Redis写入格式
        DataStream<Tuple2<String, byte[]>> redisStream = aggregatedStream
                .map(new MapFunction<AiTagParseCommon.TagPostsAggregation, Tuple2<String, byte[]>>() {
                    @Override
                    public Tuple2<String, byte[]> map(AiTagParseCommon.TagPostsAggregation agg) throws Exception {
                        String redisKey = PREFIX + agg.tagId + SUFFIX;

                        // 构建Protobuf对象
                        RecFeature.RecTagPostsFeature.Builder builder = RecFeature.RecTagPostsFeature.newBuilder();
                        builder.addAllPostIds(agg.postIds);

                        byte[] value = builder.build().toByteArray();
                        return new Tuple2<>(redisKey, value);
                    }
                })
                .name("Convert to Redis Format");

        // 创建Redis sink
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(86400); // 设置1天TTL
        RedisUtil.addRedisSink(
                redisStream,
                redisConfig,
                true,   // 异步写入
                100     // 批量大小
        );




        // 执行任务
        env.execute("Item Feature 24h Job");
    }














}
