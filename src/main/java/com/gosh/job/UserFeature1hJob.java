package com.gosh.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class UserFeature1hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature1hJob.class);
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post1h";
    // 每个窗口内每个用户的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 100;

    public static void main(String[] args) throws Exception {
        System.out.println("Starting UserFeature1hJob...");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        // 设置全局并行度为1
        env.setParallelism(3);
        System.out.println("Flink environment created with parallelism: " + env.getParallelism());
        
        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "post"
        );
        System.out.println("Kafka source created for topic: post");
        LOG.info("loglog----------Kafka source created for topic: post");

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source"
        );
        System.out.println("Kafka source stream created");

        // 3.0 预过滤 - 只保留我们需要的事件类型
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(16, 8))
            .name("Pre-filter Events");

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<UserFeatureCommon.PostExposeEvent> exposeStream = filteredStream
            .flatMap(new UserFeatureCommon.ExposeEventParser())
            .name("Parse Expose Events");

        // 增加曝光事件计数日志（每分钟采样一次）
//        exposeStream = exposeStream
//            .process(new ProcessFunction<UserFeatureCommon.PostExposeEvent, UserFeatureCommon.PostExposeEvent>() {
//                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
//                private transient long lastSampleTime;
//                private transient long counter;
//
//                @Override
//                public void open(Configuration parameters) throws Exception {
//                    lastSampleTime = 0L;
//                    counter = 0L;
//                }
//
//                @Override
//                public void processElement(UserFeatureCommon.PostExposeEvent value, Context ctx, Collector<UserFeatureCommon.PostExposeEvent> out) throws Exception {
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
//                        if (lastSampleTime != 0L) {
//                            LOG.info("[Expose Events] count in last minute: {}", counter);
//                        }
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
//                        counter = 0L;
//                    }
//                    counter++;
//                    out.collect(value);
//                }
//            })
//            .name("Debug Expose Event Count");

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
            .flatMap(new UserFeatureCommon.ViewEventParser())
            .name("Parse View Events");

        // 增加观看事件计数日志（每分钟采样一次）
//        viewStream = viewStream
//            .process(new ProcessFunction<UserFeatureCommon.PostViewEvent, UserFeatureCommon.PostViewEvent>() {
//                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
//                private transient long lastSampleTime;
//                private transient long counter;
//
//                @Override
//                public void open(Configuration parameters) throws Exception {
//                    lastSampleTime = 0L;
//                    counter = 0L;
//                }
//
//                @Override
//                public void processElement(UserFeatureCommon.PostViewEvent value, Context ctx, Collector<UserFeatureCommon.PostViewEvent> out) throws Exception {
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
//                        if (lastSampleTime != 0L) {
//                            LOG.info("[View Events] count in last minute: {}", counter);
//                        }
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
//                        counter = 0L;
//                    }
//                    counter++;
//                    out.collect(value);
//                }
//            })
//            .name("Debug View Event Count");

        // 3.3 将曝光事件转换为统一的用户特征事件
        DataStream<UserFeatureCommon.UserFeatureEvent> exposeFeatureStream = exposeStream
            .flatMap(new UserFeatureCommon.ExposeToFeatureMapper())
            .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的用户特征事件
        DataStream<UserFeatureCommon.UserFeatureEvent> viewFeatureStream = viewStream
            .flatMap(new UserFeatureCommon.ViewToFeatureMapper())
            .name("View to Feature");

        // 3.5 合并两个流
        DataStream<UserFeatureCommon.UserFeatureEvent> unifiedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureCommon.UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            );

        // 第四步：按用户ID分组并进行滑动窗口聚合
        DataStream<UserFeatureAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureCommon.UserFeatureEvent, Long>() {
                @Override
                public Long getKey(UserFeatureCommon.UserFeatureEvent value) throws Exception {
                    return value.getUid();
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(60),  // 窗口大小1小时，便于测试
                Time.minutes(2)   // 滑动间隔2分钟
            ))
            .aggregate(new UserFeatureAggregator())
            .name("User Feature Aggregation");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<UserFeatureAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserFeatureAggregation agg) throws Exception {
                    // 构建Redis key
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    
                    // 构建Protobuf
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.newBuilder()
                        // 1小时曝光特征
                        .setViewerExppostCnt1H(agg.viewerExppostCnt1h)
                        .setViewerExp1PostCnt1H(agg.viewerExp1PostCnt1h)
                        .setViewerExp2PostCnt1H(agg.viewerExp2PostCnt1h)
                        // 1小时观看特征
                        .setViewer3SviewPostCnt1H(agg.viewer3sviewPostCnt1h)
                        .setViewer3Sview1PostCnt1H(agg.viewer3sview1PostCnt1h)
                        .setViewer3Sview2PostCnt1H(agg.viewer3sview2PostCnt1h)
                        // 1小时历史记录特征
                        .setViewer3SviewPostHis1H(agg.viewer3sviewPostHis1h)
                        .setViewer5SstandPostHis1H(agg.viewer5sstandPostHis1h)
                        .setViewerLikePostHis1H(agg.viewerLikePostHis1h)
                        .setViewerFollowPostHis1H(agg.viewerFollowPostHis1h)
                        .setViewerProfilePostHis1H(agg.viewerProfilePostHis1h)
                        .setViewerPosinterPostHis1H(agg.viewerPosinterPostHis1h)
                        .build();

                    byte[] value = feature.toByteArray();
                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 打印聚合结果用于调试（采样）
//        aggregatedStream
//            .process(new ProcessFunction<UserFeatureAggregation, UserFeatureAggregation>() {
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
//                public void processElement(UserFeatureAggregation value, Context ctx, Collector<UserFeatureAggregation> out) throws Exception {
//                    // 采样日志
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL); // 对齐到分钟
//                        sampleCount = 0;
//                    }
//
//                    // 只采样前3条
//                    if (sampleCount < SAMPLE_COUNT) {
//                        sampleCount++;
//                        LOG.info("[Sample {}/{}] User {} at {}: expose={}, 3s_views={}",
//                            sampleCount,
//                            SAMPLE_COUNT,
//                            value.uid,
//                            new SimpleDateFormat("HH:mm:ss").format(new Date(value.updateTime)),
//                            value.viewerExppostCnt1h,
//                            value.viewer3sviewPostCnt1h);
//                    }
//                }
//            })
//            .name("Debug Sampling");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(600);
        System.out.println("Redis config created with TTL: " + redisConfig.getTtl());
        
        RedisUtil.addRedisSink(
            dataStream,
            redisConfig,
            true, // 异步写入
            100   // 批量大小
        );
        System.out.println("Redis sink added to the pipeline");

        // 执行任务
        System.out.println("Starting Flink job execution...");
        env.execute("User Feature 1h Job");
    }



    /**
     * 用户特征聚合器
     */
    public static class UserFeatureAggregator implements AggregateFunction<UserFeatureCommon.UserFeatureEvent, UserFeatureCommon.UserFeatureAccumulator, UserFeatureAggregation> {
        @Override
        public UserFeatureCommon.UserFeatureAccumulator createAccumulator() {
            UserFeatureCommon.UserFeatureAccumulator acc = new UserFeatureCommon.UserFeatureAccumulator();
            acc.totalEventCount = 0;
            return acc;
        }

        @Override
        public UserFeatureCommon.UserFeatureAccumulator add(UserFeatureCommon.UserFeatureEvent event, UserFeatureCommon.UserFeatureAccumulator accumulator) {
            if (accumulator.totalEventCount >= MAX_EVENTS_PER_WINDOW) {
                // 如果超过限制，直接返回当前accumulator，不再更新
                LOG.warn("User {} has exceeded the event limit ({}). Current events: {}. Skipping update.", 
                        event.getUid(), MAX_EVENTS_PER_WINDOW, accumulator.totalEventCount);
                return accumulator;
            }
            accumulator.totalEventCount++;
            return UserFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public UserFeatureAggregation getResult(UserFeatureCommon.UserFeatureAccumulator accumulator) {
            UserFeatureAggregation result = new UserFeatureAggregation();
            // 设置用户ID，确保下游Redis key正确
            result.uid = accumulator.uid;
            if (result.uid == 0L) {
                LOG.warn("UserFeatureAggregation uid is 0, check upstream event parsing and keyBy logic");
            }
            // 曝光特征
            result.viewerExppostCnt1h = accumulator.exposePostIds.size();
            // 注意：由于曝光事件中没有post_type信息，暂时无法统计图片和视频的单独曝光数
            result.viewerExp1PostCnt1h = 0; // 图片曝光数 - 需要额外数据源
            result.viewerExp2PostCnt1h = 0; // 视频曝光数 - 需要额外数据源
            
            // 观看特征
            result.viewer3sviewPostCnt1h = accumulator.view3sPostIds.size();
            result.viewer3sview1PostCnt1h = accumulator.view3s1PostIds.size();
            result.viewer3sview2PostCnt1h = accumulator.view3s2PostIds.size();
            
            // 历史记录特征 - 构建字符串格式
            result.viewer3sviewPostHis1h = UserFeatureCommon.buildPostHistoryString(accumulator.view3sPostDetails, 10);
            result.viewer5sstandPostHis1h = UserFeatureCommon.buildPostHistoryString(accumulator.stand5sPostDetails, 10);
            result.viewerLikePostHis1h = UserFeatureCommon.buildPostListString(accumulator.likePostIds, 10);
            result.viewerFollowPostHis1h = UserFeatureCommon.buildPostListString(accumulator.followPostIds, 10);
            result.viewerProfilePostHis1h = UserFeatureCommon.buildPostListString(accumulator.profilePostIds, 10);
            result.viewerPosinterPostHis1h = UserFeatureCommon.buildPostListString(accumulator.posinterPostIds, 10);
            
            result.updateTime = System.currentTimeMillis();
            
            LOG.info("Generated user feature aggregation for uid {}: {}", result.uid, result);
            return result;
        }

        @Override
        public UserFeatureCommon.UserFeatureAccumulator merge(UserFeatureCommon.UserFeatureAccumulator a, UserFeatureCommon.UserFeatureAccumulator b) {
            UserFeatureCommon.UserFeatureAccumulator merged = UserFeatureCommon.mergeAccumulators(a, b);
            merged.totalEventCount = a.totalEventCount + b.totalEventCount;
            return merged;
        }
    }

    public static class UserFeatureAggregation {
        public long uid;
        
        // 1小时特征
        public int viewerExppostCnt1h;
        public int viewerExp1PostCnt1h;  // 图片曝光数
        public int viewerExp2PostCnt1h;  // 视频曝光数
        public int viewer3sviewPostCnt1h;
        public int viewer3sview1PostCnt1h;
        public int viewer3sview2PostCnt1h;
        
        // 历史记录
        public String viewer3sviewPostHis1h;
        public String viewer5sstandPostHis1h;
        public String viewerLikePostHis1h;
        public String viewerFollowPostHis1h;
        public String viewerProfilePostHis1h;
        public String viewerPosinterPostHis1h;
        
        public long updateTime;


    }


} 