package com.gosh.job;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecTagColdFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import com.gosh.job.AiTagParseCommon.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import java.util.Objects;

public class ContentTagColdRecallJob {
    private static final Logger LOG = LoggerFactory.getLogger(ContentTagColdRecallJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String PREFIX = "rec:post:{";
    private static final String SUFFIX = "}:tag_cold_start";
    private static final String kafkaTopic = "rec";
    private static final int keepEventType = 11;
    // 每个窗口内每个tag的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 1000;


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
        SingleOutputStreamOperator<PostTagsEvent> postTagStream = filteredStream
                .flatMap(new PostTagsEventParser())
                .name("Parse ai-tag Events");

        // 3.2 将打标事件转换为基本信息单元事件
        DataStream<PostInfoEvent> postInfoStream = postTagStream
                .flatMap(new PostTagsToPostInfoMapper())
                .name("Post-Tag to Post-Info Transform");

        // 3.2 获取redis数据，同时将打标事件解析到的数据进行合并，重新写入redis




        // 按标签分组并聚合
        DataStream<TagPosts24hAggregation> aggregatedStream = postInfoStream
                .keyBy(new KeySelector<PostInfoEvent, String>() {
                    @Override
                    public String getKey(PostInfoEvent value) throws Exception {
                        // 获取第一个标签作为key
                        if (value.tag != null && !value.tag.isEmpty()) {
                            return value.tag;
                        }
                        return "";
                    }
                })
                .window(SlidingProcessingTimeWindows.of(
                        Time.hours(24), // 窗口大小24小时
                        Time.minutes(30)  // 滑动间隔半小时
                ))
                .aggregate(new TagPosts24hAggregator())
                .name("Aggregate Posts by Tag");

        // 转换为Redis写入格式
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
                .map(new MapFunction<TagPosts24hAggregation, Tuple2<String, byte[]>>() {
                    @Override
                    public Tuple2<String, byte[]> map(TagPosts24hAggregation agg) throws Exception {
                        // 构建Redis key
                        String redisKey = PREFIX + agg.tag + SUFFIX;

                        // 构建Protobuf
                        byte[] value = RecTagColdFeature.PostTagColdFeature.newBuilder()
                                // 24小时历史记录特征
                                .setTagPostInfo24H(agg.postCreatedAtHis24h)
                                .build()
                                .toByteArray();

                        return new Tuple2<>(redisKey, value);
                    }
                })
                .name("Aggregation to Protobuf Bytes");

        // 创建Redis sink
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(86400); // 设置1天TTL
        RedisUtil.addRedisSink(
                dataStream,
                redisConfig,
                true,   // 异步写入
                100     // 批量大小
        );


        // 执行任务
        env.execute("Item Feature 24h Job");
    }


    /**
     * tag-post 24小时聚合器
     */
    public static class TagPosts24hAggregator implements AggregateFunction<PostInfoEvent, TagPostsAccumulator, TagPosts24hAggregation> {
        @Override
        public TagPostsAccumulator createAccumulator() {
            TagPostsAccumulator acc = new TagPostsAccumulator();
            acc.totalEventCount = 0;
            return acc;
        }

        @Override
        public TagPostsAccumulator add(PostInfoEvent event, TagPostsAccumulator accumulator) {
            // 先设置tag，确保能写入Redis
            accumulator.tag = event.tag;

            if (accumulator.totalEventCount >= MAX_EVENTS_PER_WINDOW) {
                // 如果超过限制，标记超限状态，不再更新
                if (!accumulator.exceededLimit) {
                    accumulator.exceededLimit = true;
                }
                return accumulator;
            }
            accumulator.totalEventCount++;
            return AiTagParseCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public TagPosts24hAggregation getResult(TagPostsAccumulator accumulator) {
            TagPosts24hAggregation result = new TagPosts24hAggregation();
            // 设置tag，确保下游Redis key正确
            result.tag = accumulator.tag;
            if (Objects.equals(result.tag, "")) {
                LOG.warn("UserFeature24hAggregation tag is empty, check upstream event parsing and keyBy logic");
            }

            // 检查是否超限，如果是则打印日志
            if (accumulator.exceededLimit) {
                LOG.warn("User {} exceeded event limit ({}). Final event count: {}.",
                        accumulator.tag, MAX_EVENTS_PER_WINDOW, accumulator.totalEventCount);
            }

            // 24小时历史记录特征 - 构建字符串格式
            result.postCreatedAtHis24h = AiTagParseCommon.buildPostCreatedAtString(accumulator.postInfos, 10);
            result.updateTime = System.currentTimeMillis();
            return result;
        }

        @Override
        public TagPostsAccumulator merge(TagPostsAccumulator a, TagPostsAccumulator b) {
            TagPostsAccumulator merged = AiTagParseCommon.mergeAccumulators(a, b);
            merged.totalEventCount = a.totalEventCount + b.totalEventCount;
            return merged;
        }
    }

    /**
     * Tag-Post 24小时聚合结果
     */
    public static class TagPosts24hAggregation {
        public String tag;

        // 24小时历史记录特征
        public String postCreatedAtHis24h;

        public long updateTime;

    }


}
