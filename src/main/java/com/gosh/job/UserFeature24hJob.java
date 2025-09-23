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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;

public class UserFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature24hJob.class);
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post24h";

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        
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
            .setParallelism(8);

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<PostExposeEvent> exposeStream = filteredStream
            .flatMap(new ExposeEventParser())
            .name("Parse Expose Events")
            .setParallelism(4);

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new ViewEventParser())
            .name("Parse View Events")
            .setParallelism(4);

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

        // 打印前3次聚合结果用于调试
        aggregatedStream
            .map(new MapFunction<UserFeature24hAggregation, UserFeature24hAggregation>() {
                private int counter = 0;
                @Override
                public UserFeature24hAggregation map(UserFeature24hAggregation value) throws Exception {
                    if (counter < 3) {
                        counter++;
                        LOG.info("Sample aggregation result {}: uid={}, 24h history lists: 3sview={}, like={}, follow={}", 
                            counter, value.uid, 
                            value.viewer3sviewPostHis24h,
                            value.viewerLikePostHis24h,
                            value.viewerFollowPostHis24h);
                    }
                    return value;
                }
            })
            .name("Sample Debug Output");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<byte[]> dataStream = aggregatedStream
            .map(new MapFunction<UserFeature24hAggregation, byte[]>() {
                @Override
                public byte[] map(UserFeature24hAggregation agg) throws Exception {
                    return RecFeature.RecUserFeature.newBuilder()
                        .setUserId(agg.uid)
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
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(7200);
//        RedisUtil.addRedisSink(
//            dataStream,
//            redisConfig,
//            false, // 异步写入
//            100,  // 批量大小
//            RecFeature.RecUserFeature.class,
//            feature -> PREFIX + feature.getUserId() + SUFFIX
//        );

        kafkaSource.print();

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
            result.viewer3sviewPostHis24h = UserFeatureCommon.buildPostHistoryString(accumulator.view3sPostDetails, 20);
            result.viewer5sstandPostHis24h = UserFeatureCommon.buildPostHistoryString(accumulator.stand5sPostDetails, 20);
            result.viewerLikePostHis24h = UserFeatureCommon.buildPostListString(accumulator.likePostIds, 20);
            result.viewerFollowPostHis24h = UserFeatureCommon.buildPostListString(accumulator.followPostIds, 20);
            result.viewerProfilePostHis24h = UserFeatureCommon.buildPostListString(accumulator.profilePostIds, 20);
            result.viewerPosinterPostHis24h = UserFeatureCommon.buildPostListString(accumulator.posinterPostIds, 20);
            
            // 作者相关特征 (24小时)
            result.viewerLikeAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.likeAuthors, 20);
            result.viewerFollowAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.followAuthors, 20);
            result.viewerProfileAuthorHis24h = UserFeatureCommon.buildAuthorListString(accumulator.profileAuthors, 20);
            
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