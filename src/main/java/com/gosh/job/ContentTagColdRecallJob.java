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

// test used
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ContentTagColdRecallJob {
    private static final Logger LOG = LoggerFactory.getLogger(ContentTagColdRecallJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String PREFIX = "rec:post:{";
    private static final String SUFFIX = "}:tag_cold_start";
    private static final String kafkaTopic = "rec";
    private static final int keepEventType = 11;
    // 每个窗口内每个tag的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 1000;
    private static final boolean isDebug = false;


    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] create flink env.");
        }
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        //env.setParallelism(1);

        // 第二步：创建Source，Kafka环境
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] create kafka env.");
        }
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), kafkaTopic
        );

        // 第三步：使用KafkaSource创建DataStream
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] read/get data from kafka source.");
        }
        DataStreamSource<String> kafkaSource = env.fromSource(
                inputTopic,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source"
        );

        // 3.0 预过滤 - 只保留我们需要的事件类型
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] filtered unnecessary event_type.");
        }
        DataStream<String> filteredStream = kafkaSource
                .filter(EventFilterUtil.createFastEventTypeFilter(keepEventType))
                .name("Pre-filter Events");

        // 3.1 解析打标事件 (event_type=11)
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] PostTagsEventParser: parse info with valid event_type.");
        }
        SingleOutputStreamOperator<PostTagsEvent> postTagStream = filteredStream
                .flatMap(new PostTagsEventParser())
                .name("Parse ai-tag Events");

        // 3.2 将打标事件转换为基本信息单元事件
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] PostTagsToPostInfoMapper: transform info basic info..");
        }
        DataStream<PostInfoEvent> postInfoStream = postTagStream
                .flatMap(new PostTagsToPostInfoMapper())
                .name("Post-Tag to Post-Info Transform");

        // 无需本步骤：3.2 获取redis数据，同时将打标事件解析到的数据进行合并，重新写入redis
        // 因为这个操作的窗口是24h，无需读取再合并写入；
        // 同时来数据的时候，会根据其做时间上的判断；最后在rec线上程序使用时，跟合法的冷启post做交集即可

        // 按标签分组并聚合
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] TagPosts24hAggregator: aggregate by tag.");
        }
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
                        Time.minutes(2)  // 滑动间隔10minute
                ))
                .aggregate(new TagPosts24hAggregator())
                .name("Aggregate Posts by Tag");

        // 打印聚合结果用于调试（采样）
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] print aggregated result for debugging.");
        }
        aggregatedStream
            .process(new ProcessFunction<TagPosts24hAggregation, TagPosts24hAggregation>() {
                private static final long SAMPLE_INTERVAL = 60000; // 采样间隔1分钟
                private static final int SAMPLE_COUNT = 3; // 每次采样3条
                private transient long lastSampleTime;
                private transient int sampleCount;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0;
                    sampleCount = 0;
                }

                @Override
                public void processElement(TagPosts24hAggregation value, Context ctx, Collector<TagPosts24hAggregation> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Sample {}/{}] tag {} at {}: postCreatedAtHis24h={}",
                            sampleCount,
                            SAMPLE_COUNT,
                            value.tag,
                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
                            value.postCreatedAtHis24h);
                    }
                }
            })
            .name("Debug Sampling");


        // 转换为Protobuf并写入Redis
        if(isDebug) {
            LOG.info("[ContentTagColdRecallJob] write to redis.");
        }
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
                .map(new MapFunction<TagPosts24hAggregation, Tuple2<String, byte[]>>() {
                    @Override
                    public Tuple2<String, byte[]> map(TagPosts24hAggregation agg) throws Exception {
                        // 构建Redis key
                        String redisKey = PREFIX + agg.tag + SUFFIX;
                        if(isDebug) {
                            LOG.info("[ContentTagColdRecallJob] write to redis: {}", redisKey);
                        }

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
        // if(isDebug) {
        //    LOG.info("[ContentTagColdRecallJob] create redis sink.");
        // }
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(86400); // 设置1天TTL
        RedisUtil.addRedisSink(
                dataStream,
                redisConfig,
                true,   // 异步写入
                100     // 批量大小
        );

        // if(isDebug) {
        //    LOG.info("[ContentTagColdRecallJob] execute job.");
        // }
        // 执行任务
        env.execute("ContentTagColdRecallJob 24h");
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
                LOG.warn("ContentTagCold24hAggregation tag is empty, check upstream event parsing and keyBy logic");
            }

            // 检查是否超限，如果是则打印日志
            if (accumulator.exceededLimit) {
                LOG.warn("Tag {} exceeded event limit ({}). Final event count: {}.",
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
