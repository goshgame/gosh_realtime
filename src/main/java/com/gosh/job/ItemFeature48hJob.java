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

public class ItemFeature48hJob {
    private static final Logger LOG = LoggerFactory.getLogger(ItemFeature48hJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:item_feature:{";
    private static String SUFFIX = "}:post48h";

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

        // 第四步：按post_id分组并进行48小时滑动窗口聚合
        DataStream<ItemFeature48hAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureEvent, Long>() {
                @Override
                public Long getKey(UserFeatureEvent value) throws Exception {
                    return value.postId;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(48), // 窗口大小48小时
                Time.minutes(30) // 滑动间隔30分钟
            ))
            .aggregate(new ItemFeature48hAggregator())
            .name("Item Feature 48h Aggregation");

        // 打印聚合结果用于调试（采样）
//        aggregatedStream
//            .process(new ProcessFunction<ItemFeature48hAggregation, ItemFeature48hAggregation>() {
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
//                public void processElement(ItemFeature48hAggregation value, Context ctx, Collector<ItemFeature48hAggregation> out) throws Exception {
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
//                        sampleCount = 0;
//                    }
//                    if (sampleCount < SAMPLE_COUNT) {
//                        sampleCount++;
//                        LOG.info("[Sample {}/{}] postId {} at {}: exp48h={}, 3sview48h={}, 8sview48h={}, like48h={}",
//                            sampleCount,
//                            SAMPLE_COUNT,
//                            value.postId,
//                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
//                            value.postExpCnt48h,
//                            value.post3sviewCnt48h,
//                            value.post8sviewCnt48h,
//                            value.postLikeCnt48h);
//                    }
//                }
//            })
//            .name("Debug Sampling");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<ItemFeature48hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(ItemFeature48hAggregation agg) throws Exception {
                    // 构建Redis key
                    String redisKey = PREFIX + agg.postId + SUFFIX;
                    
                    // 构建Protobuf
                    byte[] value = RecFeature.RecPostFeature.newBuilder()
                        // 48小时曝光特征
                        .setPostExpCnt48H(agg.postExpCnt48h)
                        // 48小时观看特征
                        .setPost3SviewCnt48H(agg.post3sviewCnt48h)
                        .setPost8SviewCnt48H(agg.post8sviewCnt48h)
                        .setPost12SviewCnt48H(agg.post12sviewCnt48h)
                        .setPost20SviewCnt48H(agg.post20sviewCnt48h)
                        // 48小时停留特征
                        .setPost5SstandCnt48H(agg.post5sstandCnt48h)
                        .setPost10SstandCnt48H(agg.post10sstandCnt48h)
                        // 48小时互动特征
                        .setPostLikeCnt48H(agg.postLikeCnt48h)
                        .setPostFollowCnt48H(agg.postFollowCnt48h)
                        .setPostProfileCnt48H(agg.postProfileCnt48h)
                        .setPostPosinterCnt48H(agg.postPosinterCnt48h)
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
        env.execute("Item Feature 48h Job");
    }

    /**
     * Item特征48小时聚合器
     */
    public static class ItemFeature48hAggregator implements AggregateFunction<UserFeatureEvent, ItemFeatureAccumulator, ItemFeature48hAggregation> {
        @Override
        public ItemFeatureAccumulator createAccumulator() {
            return new ItemFeatureAccumulator();
        }

        @Override
        public ItemFeatureAccumulator add(UserFeatureEvent event, ItemFeatureAccumulator accumulator) {
            return ItemFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public ItemFeature48hAggregation getResult(ItemFeatureAccumulator accumulator) {
            ItemFeature48hAggregation result = new ItemFeature48hAggregation();
            result.postId = accumulator.postId;
            
            // 曝光特征
            result.postExpCnt48h = (int) accumulator.exposeHLL.cardinality();
            
            // 观看特征
            result.post3sviewCnt48h = (int) accumulator.view3sHLL.cardinality();
            result.post8sviewCnt48h = (int) accumulator.view8sHLL.cardinality();
            result.post12sviewCnt48h = (int) accumulator.view12sHLL.cardinality();
            result.post20sviewCnt48h = (int) accumulator.view20sHLL.cardinality();
            
            // 停留特征
            result.post5sstandCnt48h = (int) accumulator.stand5sHLL.cardinality();
            result.post10sstandCnt48h = (int) accumulator.stand10sHLL.cardinality();
            
            // 互动特征
            result.postLikeCnt48h = (int) accumulator.likeHLL.cardinality();
            result.postFollowCnt48h = (int) accumulator.followHLL.cardinality();
            result.postProfileCnt48h = (int) accumulator.profileHLL.cardinality();
            result.postPosinterCnt48h = (int) accumulator.posinterHLL.cardinality();
            
            return result;
        }

        @Override
        public ItemFeatureAccumulator merge(ItemFeatureAccumulator a, ItemFeatureAccumulator b) {
            return ItemFeatureCommon.mergeAccumulators(a, b);
        }
    }

    /**
     * Item特征48小时聚合结果
     */
    public static class ItemFeature48hAggregation {
        public long postId;
        
        // 48小时曝光特征
        public int postExpCnt48h;
        
        // 48小时观看特征
        public int post3sviewCnt48h;
        public int post8sviewCnt48h;
        public int post12sviewCnt48h;
        public int post20sviewCnt48h;
        
        // 48小时停留特征
        public int post5sstandCnt48h;
        public int post10sstandCnt48h;
        
        // 48小时互动特征
        public int postLikeCnt48h;
        public int postFollowCnt48h;
        public int postProfileCnt48h;
        public int postPosinterCnt48h;
    }
} 