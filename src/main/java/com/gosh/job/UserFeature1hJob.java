package com.gosh.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
import com.gosh.util.*;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

public class UserFeature1hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature1hJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post1h";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting UserFeature1hJob...");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        // 设置全局并行度为1
        env.setParallelism(1);
        System.out.println("Flink environment created with parallelism: " + env.getParallelism());
        
        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "post"
        );
        System.out.println("Kafka source created for topic: post");

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
            .name("Pre-filter Events")
            .setParallelism(1);

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<UserFeatureCommon.PostExposeEvent> exposeStream = filteredStream
            .flatMap(new UserFeatureCommon.ExposeEventParser())
            .name("Parse Expose Events")
            .setParallelism(1);

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
            .flatMap(new UserFeatureCommon.ViewEventParser())
            .name("Parse View Events")
            .setParallelism(1);

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

        // 打印聚合结果用于调试
        aggregatedStream
            .map(new MapFunction<UserFeatureAggregation, UserFeatureAggregation>() {
                private long totalCount = 0;
                private int printedCount = 0;
                private long lastLogTime = 0;
                private static final int MAX_PRINT = 3;
                private static final long LOG_INTERVAL = 60000; // 每分钟打印一次统计
                
                @Override
                public UserFeatureAggregation map(UserFeatureAggregation value) throws Exception {
                    totalCount++;
                    long now = System.currentTimeMillis();
                    
                    // 每分钟打印一次统计信息
                    if (now - lastLogTime > LOG_INTERVAL) {
                        System.out.println(String.format("\n=== Stats at %s ===", 
                            new SimpleDateFormat("HH:mm:ss").format(new Date(now))));
                        System.out.println("Total records processed: " + totalCount);
                        lastLogTime = now;
                        // 重置打印计数
                        printedCount = 0;
                    }

                    // 只打印前3条
                    if (printedCount < MAX_PRINT) {
                        printedCount++;
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("[%d/%d] User %d Stats:\n", 
                            printedCount, MAX_PRINT,
                            value.uid));

                        // 只有当有数据时才打印相应的统计
                        if (value.viewerExppostCnt1h > 0) {
                            sb.append(String.format("- Expose: %d posts\n", value.viewerExppostCnt1h));
                        }
                        if (value.viewer3sviewPostCnt1h > 0) {
                            sb.append(String.format("- 3s Views: %d posts\n", value.viewer3sviewPostCnt1h));
                        }
                        if (!value.viewerLikePostHis1h.isEmpty()) {
                            sb.append("- Like history: ").append(value.viewerLikePostHis1h).append("\n");
                        }
                        if (!value.viewerFollowPostHis1h.isEmpty()) {
                            sb.append("- Follow history: ").append(value.viewerFollowPostHis1h).append("\n");
                        }

                        System.out.println(sb.toString());
                    }
                    
                    return value;
                }
            })
            .name("Debug Output");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<UserFeatureAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserFeatureAggregation agg) throws Exception {
                    // 构建Redis key
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    
                    // 构建Protobuf
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.newBuilder()
                        .setUserId(agg.uid)
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
                    
                    System.out.println("Preparing to write to Redis - Key: " + redisKey);
                    System.out.println("Feature data: " + feature.toString());
                    
                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(600);
        System.out.println("Redis config created with TTL: " + redisConfig.getTtl());
        
        RedisUtil.addRedisSink(
            dataStream,
            redisConfig,
            false, // 异步写入
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
            return new UserFeatureCommon.UserFeatureAccumulator();
        }

        @Override
        public UserFeatureCommon.UserFeatureAccumulator add(UserFeatureCommon.UserFeatureEvent event, UserFeatureCommon.UserFeatureAccumulator accumulator) {
            return UserFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public UserFeatureAggregation getResult(UserFeatureCommon.UserFeatureAccumulator accumulator) {
            UserFeatureAggregation result = new UserFeatureAggregation();
            result.uid = accumulator.uid;
            
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
            result.viewer3sviewPostHis1h = UserFeatureCommon.buildPostHistoryString(accumulator.view3sPostDetails, 20);
            result.viewer5sstandPostHis1h = UserFeatureCommon.buildPostHistoryString(accumulator.stand5sPostDetails, 20);
            result.viewerLikePostHis1h = UserFeatureCommon.buildPostListString(accumulator.likePostIds, 20);
            result.viewerFollowPostHis1h = UserFeatureCommon.buildPostListString(accumulator.followPostIds, 20);
            result.viewerProfilePostHis1h = UserFeatureCommon.buildPostListString(accumulator.profilePostIds, 20);
            result.viewerPosinterPostHis1h = UserFeatureCommon.buildPostListString(accumulator.posinterPostIds, 20);
            
            result.updateTime = System.currentTimeMillis();
            
            LOG.info("Generated user feature aggregation for uid {}: {}", result.uid, result);
            return result;
        }

        @Override
        public UserFeatureCommon.UserFeatureAccumulator merge(UserFeatureCommon.UserFeatureAccumulator a, UserFeatureCommon.UserFeatureAccumulator b) {
            return UserFeatureCommon.mergeAccumulators(a, b);
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