package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
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

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UserFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature24hJob.class);
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post24h";

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        env.setParallelism(1);
        
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

        // 3.0 预过滤 - 使用通用过滤工具，只保留我们需要的事件类型
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(16, 8))
            .name("Pre-filter Events")
            .setParallelism(1);

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<PostExposeEvent> exposeStream = filteredStream
            .flatMap(new ExposeEventParser())
            .name("Parse Expose Events")
            .setParallelism(1);

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new ViewEventParser())
            .name("Parse View Events")
            .setParallelism(1);

        // 3.3 将曝光事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> exposeFeatureStream = exposeStream
            .flatMap(new ExposeToFeatureMapper())
            .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> viewFeatureStream = viewStream
            .flatMap(new ViewToFeatureMapper())
            .name("View to Feature");

        // 3.5 合并两个流
        DataStream<UserFeatureEvent> unifiedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            );

        // 第四步：按用户ID分组并进行24小时滑动窗口聚合
        DataStream<UserFeature24hAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureEvent, Long>() {
                @Override
                public Long getKey(UserFeatureEvent value) throws Exception {
                    return value.getUid();
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(24), // 窗口大小24小时
                Time.minutes(30)   // 滑动间隔半小时
            ))
            .aggregate(new UserFeature24hAggregator())
            .name("User Feature 24h Aggregation");

        // 打印聚合结果用于调试（采样）
        aggregatedStream
            .process(new ProcessFunction<UserFeature24hAggregation, UserFeature24hAggregation>() {
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
                public void processElement(UserFeature24hAggregation value, Context ctx, Collector<UserFeature24hAggregation> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Sample {}/{}] uid {} at {}: 3sviewHis24h={}, likeHis24h={}, followHis24h={}", 
                            sampleCount, 
                            SAMPLE_COUNT,
                            value.uid,
                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
                            value.viewer3sviewPostHis24h,
                            value.viewerLikePostHis24h,
                            value.viewerFollowPostHis24h);
                    }
                }
            })
            .name("Debug Sampling");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<UserFeature24hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserFeature24hAggregation agg) throws Exception {
                    // 构建Redis key
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    
                    // 构建Protobuf
                    byte[] value = RecFeature.RecUserFeature.newBuilder()
                        // 24小时历史记录特征
                        .setViewer3SviewPostHis24H(agg.viewer3sviewPostHis24h)
                        .setViewer5SstandPostHis24H(agg.viewer5sstandPostHis24h)
                        .setViewerLikePostHis24H(agg.viewerLikePostHis24h)
                        .setViewerFollowPostHis24H(agg.viewerFollowPostHis24h)
                        .setViewerProfilePostHis24H(agg.viewerProfilePostHis24h)
                        .setViewerPosinterPostHis24H(agg.viewerPosinterPostHis24h)
                        // 作者相关特征 (24小时)
                        .setViewerLikeAuthorHis24H(agg.viewerLikeAuthorHis24h)
                        .setViewerFollowAuthorHis24H(agg.viewerFollowAuthorHis24h)
                        .setViewerProfileAuthorHis24H(agg.viewerProfileAuthorHis24h)
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
            false, // 异步写入
            100   // 批量大小
        );

        // 执行任务
        env.execute("User Feature 24h Job");
    }

    /**
     * 用户特征24小时聚合器
     */
    public static class UserFeature24hAggregator implements AggregateFunction<UserFeatureEvent, UserFeatureAccumulator, UserFeature24hAggregation> {
        @Override
        public UserFeatureAccumulator createAccumulator() {
            return new UserFeatureAccumulator();
        }

        @Override
        public UserFeatureAccumulator add(UserFeatureEvent event, UserFeatureAccumulator accumulator) {
            return UserFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public UserFeature24hAggregation getResult(UserFeatureAccumulator accumulator) {
            UserFeature24hAggregation result = new UserFeature24hAggregation();
            result.uid = accumulator.uid;
            
            // 24小时历史记录特征 - 构建字符串格式
            result.viewer3sviewPostHis24h = UserFeatureCommon.buildPostHistoryString(accumulator.view3sPostDetails, 10);
            result.viewer5sstandPostHis24h = UserFeatureCommon.buildPostHistoryString(accumulator.stand5sPostDetails, 10);
            result.viewerLikePostHis24h = UserFeatureCommon.buildPostListString(accumulator.likePostIds, 10);
            result.viewerFollowPostHis24h = UserFeatureCommon.buildPostListString(accumulator.followPostIds, 10);
            result.viewerProfilePostHis24h = UserFeatureCommon.buildPostListString(accumulator.profilePostIds, 10);
            result.viewerPosinterPostHis24h = UserFeatureCommon.buildPostListString(accumulator.posinterPostIds, 10);
            
            // 作者相关特征 (24小时)
            result.viewerLikeAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.likeAuthors, 10);
            result.viewerFollowAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.followAuthors, 10);
            result.viewerProfileAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.profileAuthors, 10);
            
            result.updateTime = System.currentTimeMillis();
            
            LOG.info("Generated user 24h feature aggregation for uid {}: {}", result.uid, result);
            return result;
        }

        @Override
        public UserFeatureAccumulator merge(UserFeatureAccumulator a, UserFeatureAccumulator b) {
            return UserFeatureCommon.mergeAccumulators(a, b);
        }
    }

    /**
     * 用户特征24小时聚合结果
     */
    public static class UserFeature24hAggregation {
        public long uid;
        
        // 24小时历史记录特征
        public String viewer3sviewPostHis24h;
        public String viewer5sstandPostHis24h;
        public String viewerLikePostHis24h;
        public String viewerFollowPostHis24h;
        public String viewerProfilePostHis24h;
        public String viewerPosinterPostHis24h;
        
        // 作者相关特征 (24小时)
        public String viewerLikeAuthorHis24h;
        public String viewerFollowAuthorHis24h;
        public String viewerProfileAuthorHis24h;
        
        public long updateTime;


    }


} 