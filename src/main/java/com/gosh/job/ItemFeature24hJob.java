package com.gosh.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
import com.gosh.job.ItemFeatureCommon.*;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.time.Duration;

public class ItemFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(ItemFeature24hJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:item_feature:{";
    private static String SUFFIX = "}:post24h";
    // 每个窗口内每个item的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 200000;

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
//        env.setParallelism(1);
        
        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "post"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留我们需要的事件类型
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(16, 8))
            .name("Pre-filter Events");

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<PostExposeEvent> exposeStream = filteredStream
            .flatMap(new UserFeatureCommon.ExposeEventParser())
            .name("Parse Expose Events");

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new UserFeatureCommon.ViewEventParser())
            .name("Parse View Events");

        // 3.3 将曝光事件转换为统一的特征事件
        DataStream<UserFeatureEvent> exposeFeatureStream = exposeStream
            .flatMap(new UserFeatureCommon.ExposeToFeatureMapper())
            .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的特征事件
        DataStream<UserFeatureEvent> viewFeatureStream = viewStream
            .flatMap(new UserFeatureCommon.ViewToFeatureMapper())
            .name("View to Feature");

        // 3.5 合并两个流
        DataStream<UserFeatureEvent> unifiedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            );

        // 第四步：按post_id分组并进行24小时滑动窗口聚合
        DataStream<ItemFeature24hAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureEvent, Long>() {
                @Override
                public Long getKey(UserFeatureEvent value) throws Exception {
                    return value.postId;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(24), // 窗口大小24小时
                Time.minutes(30) // 滑动间隔30分钟
            ))
            .aggregate(new ItemFeature24hAggregator())
            .name("Item Feature 24h Aggregation");

        // 打印聚合结果用于调试（采样）
//        aggregatedStream
//            .process(new ProcessFunction<ItemFeature24hAggregation, ItemFeature24hAggregation>() {
//                private static final long SAMPLE_INTERVAL = 60000; // 采样间隔1分钟
//                private static final int SAMPLE_COUNT = 3; // 每次采样3条
//                private transient long lastSampleTime;
//                private transient int sampleCount;
//
//                @Override
//                public void open(Configuration parameters) throws Exception {
//                    lastSampleTime = 0;
//                    sampleCount = 0;
//                }
//
//                @Override
//                public void processElement(ItemFeature24hAggregation value, Context ctx, Collector<ItemFeature24hAggregation> out) throws Exception {
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
//                        sampleCount = 0;
//                    }
//                    if (sampleCount < SAMPLE_COUNT) {
//                        sampleCount++;
//                        LOG.info("[Sample {}/{}] postId {} at {}: exp24h={}, 3sview24h={}, 8sview24h={}, like24h={}",
//                            sampleCount,
//                            SAMPLE_COUNT,
//                            value.postId,
//                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
//                            value.postExpCnt24h,
//                            value.post3sviewCnt24h,
//                            value.post8sviewCnt24h,
//                            value.postLikeCnt24h);
//                    }
//                }
//            })
//            .name("Debug Sampling");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<ItemFeature24hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(ItemFeature24hAggregation agg) throws Exception {
                    // 构建Redis key
                    String redisKey = PREFIX + agg.postId + SUFFIX;
                    
                    // 构建Protobuf
                    byte[] value = RecFeature.RecPostFeature.newBuilder()
                        // 24小时曝光特征
                        .setPostExpCnt24H(agg.postExpCnt24h)
                        // 24小时观看特征
                        .setPost3SviewCnt24H(agg.post3sviewCnt24h)
                        .setPost8SviewCnt24H(agg.post8sviewCnt24h)
                        .setPost12SviewCnt24H(agg.post12sviewCnt24h)
                        .setPost20SviewCnt24H(agg.post20sviewCnt24h)
                        // 24小时停留特征
                        .setPost5SstandCnt24H(agg.post5sstandCnt24h)
                        .setPost10SstandCnt24H(agg.post10sstandCnt24h)
                        // 24小时互动特征
                        .setPostLikeCnt24H(agg.postLikeCnt24h)
                        .setPostFollowCnt24H(agg.postFollowCnt24h)
                        .setPostProfileCnt24H(agg.postProfileCnt24h)
                        .setPostPosinterCnt24H(agg.postPosinterCnt24h)
                        .build()
                        .toByteArray();
                    
                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(7200);
        RedisUtil.addRedisSink(
            dataStream,
            redisConfig,
            true, // 异步写入
            100   // 批量大小
        );

        // 执行任务
        env.execute("Item Feature 24h Job");
    }

    /**
     * Item特征24小时聚合器
     */
    public static class ItemFeature24hAggregator implements AggregateFunction<UserFeatureEvent, ItemFeatureAccumulator, ItemFeature24hAggregation> {
        @Override
        public ItemFeatureAccumulator createAccumulator() {
            return new ItemFeatureAccumulator();
        }

        @Override
        public ItemFeatureAccumulator add(UserFeatureEvent event, ItemFeatureAccumulator accumulator) {
            // 先设置postId，确保即使跳过也能写入Redis
            accumulator.postId = event.postId;
            
            if (accumulator.totalEventCount >= MAX_EVENTS_PER_WINDOW) {
                // 如果超过限制，记录一次警告并跳过，但不影响现有数据的写入
                if (accumulator.totalEventCount == MAX_EVENTS_PER_WINDOW) {
                    LOG.warn("Post {} has exceeded the event limit ({}). Further events will be skipped.", 
                            event.postId, MAX_EVENTS_PER_WINDOW);
                }
                return accumulator;
            }

            return ItemFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public ItemFeature24hAggregation getResult(ItemFeatureAccumulator accumulator) {
            ItemFeature24hAggregation result = new ItemFeature24hAggregation();
            result.postId = accumulator.postId;
            
            // 曝光特征
            result.postExpCnt24h = (int) accumulator.exposeHLL.cardinality();
            
            // 观看特征
            result.post3sviewCnt24h = (int) accumulator.view3sHLL.cardinality();
            result.post8sviewCnt24h = (int) accumulator.view8sHLL.cardinality();
            result.post12sviewCnt24h = (int) accumulator.view12sHLL.cardinality();
            result.post20sviewCnt24h = (int) accumulator.view20sHLL.cardinality();
            
            // 停留特征
            result.post5sstandCnt24h = (int) accumulator.stand5sHLL.cardinality();
            result.post10sstandCnt24h = (int) accumulator.stand10sHLL.cardinality();
            
            // 互动特征
            result.postLikeCnt24h = (int) accumulator.likeHLL.cardinality();
            result.postFollowCnt24h = (int) accumulator.followHLL.cardinality();
            result.postProfileCnt24h = (int) accumulator.profileHLL.cardinality();
            result.postPosinterCnt24h = (int) accumulator.posinterHLL.cardinality();
            
            return result;
        }

        @Override
        public ItemFeatureAccumulator merge(ItemFeatureAccumulator a, ItemFeatureAccumulator b) {
            return ItemFeatureCommon.mergeAccumulators(a, b);
        }
    }

    /**
     * Item特征24小时聚合结果
     */
    public static class ItemFeature24hAggregation {
        public long postId;
        
        // 24小时曝光特征
        public int postExpCnt24h;
        
        // 24小时观看特征
        public int post3sviewCnt24h;
        public int post8sviewCnt24h;
        public int post12sviewCnt24h;
        public int post20sviewCnt24h;
        
        // 24小时停留特征
        public int post5sstandCnt24h;
        public int post10sstandCnt24h;
        
        // 24小时互动特征
        public int postLikeCnt24h;
        public int postFollowCnt24h;
        public int postProfileCnt24h;
        public int postPosinterCnt24h;
    }
} 