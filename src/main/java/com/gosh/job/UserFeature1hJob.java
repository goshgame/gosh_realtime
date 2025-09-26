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
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class UserFeature1hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature1hJob.class);
    private static final Logger MONITOR = LoggerFactory.getLogger("com.gosh.job.UserFeature1hJob.monitor");
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post1h";
    private static String AUTHOR_PREFIX = "rec:user_author_feature:{";
    private static String AUTHOR_SUFFIX = "}:post1h";
    private static final int MAX_EVENTS_PER_KEY = 100;

    private static class WindowState {
        int eventCount;
        UserFeatureCommon.UserFeatureAccumulator userAccumulator;
        Map<Long, UserAuthorFeature1hJob.UserAuthorAccumulator> authorAccumulators; // key: authorId
        long windowStart;
        
        WindowState(long windowStart) {
            this.eventCount = 0;
            this.userAccumulator = new UserFeatureCommon.UserFeatureAccumulator();
            this.authorAccumulators = new HashMap<>();
            this.windowStart = windowStart;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting UserFeature1hJob...");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        // 设置全局并行度为1
        env.setParallelism(3);
        System.out.println("Flink environment created with parallelism: " + env.getParallelism());

        // 打印日志级别
        System.out.println("Log level check - DEBUG: " + LOG.isDebugEnabled() + 
            ", INFO: " + LOG.isInfoEnabled() + 
            ", WARN: " + LOG.isWarnEnabled() + 
            ", ERROR: " + LOG.isErrorEnabled());
        LOG.error("Log level check via LOG.error");
        LOG.warn("Log level check via LOG.warn");
        LOG.info("Log level check via LOG.info");
        LOG.debug("Log level check via LOG.debug");
        
        MONITOR.error("Monitor log level check via ERROR");
        MONITOR.warn("Monitor log level check via WARN");
        MONITOR.info("Monitor log level check via INFO");
        MONITOR.debug("Monitor log level check via DEBUG");
        
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
        exposeStream = exposeStream
            .process(new ProcessFunction<UserFeatureCommon.PostExposeEvent, UserFeatureCommon.PostExposeEvent>() {
                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
                private transient long lastSampleTime;
                private transient long counter;
                private transient int subtaskIndex;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0L;
                    counter = 0L;
                    subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                    String msg = "Expose counter opened: subtask=" + (subtaskIndex + 1);
                    System.out.println(msg);
                    LOG.error(msg); // 使用 ERROR 级别确保可见
                }

                @Override
                public void processElement(UserFeatureCommon.PostExposeEvent value, Context ctx, Collector<UserFeatureCommon.PostExposeEvent> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
                        if (lastSampleTime != 0L) {
                            String msg = String.format("[Counter] expose_events=%d, subtask=%d", counter, subtaskIndex + 1);
                            System.out.println(msg);
                            LOG.error(msg); // 使用 ERROR 级别确保可见
                        }
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        counter = 0L;
                    }
                    counter++;
                    out.collect(value);
                }
            })
            .name("Debug Expose Event Count");

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
            .flatMap(new UserFeatureCommon.ViewEventParser())
            .name("Parse View Events");

        // 增加观看事件计数日志（每分钟采样一次）
        viewStream = viewStream
            .process(new ProcessFunction<UserFeatureCommon.PostViewEvent, UserFeatureCommon.PostViewEvent>() {
                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
                private transient long lastSampleTime;
                private transient long counter;
                private transient int subtaskIndex;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0L;
                    counter = 0L;
                    subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                    String msg = "View counter opened: subtask=" + (subtaskIndex + 1);
                    System.out.println(msg);
                    LOG.error(msg); // 使用 ERROR 级别确保可见
                }

                @Override
                public void processElement(UserFeatureCommon.PostViewEvent value, Context ctx, Collector<UserFeatureCommon.PostViewEvent> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
                        if (lastSampleTime != 0L) {
                            String msg = String.format("[Counter] view_events=%d, subtask=%d", counter, subtaskIndex + 1);
                            System.out.println(msg);
                            LOG.error(msg); // 使用 ERROR 级别确保可见
                        }
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        counter = 0L;
                    }
                    counter++;
                    out.collect(value);
                }
            })
            .name("Debug View Event Count");

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

        // 4.1 用户特征聚合
        DataStream<UserFeatureAggregation> userFeatureStream = unifiedStream
            .keyBy(event -> event.getUid())
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(1),  // 窗口大小1小时
                Time.minutes(2) // 滑动间隔2分钟
            ))
            .aggregate(new UserFeatureAggregator())
            .name("User Feature Aggregation");

        // 4.2 用户-作者特征聚合
        DataStream<Tuple2<Long, Map<String, byte[]>>> userAuthorFeatureStream = unifiedStream
            .keyBy(event -> new Tuple2<>(event.getUid(), event.author))
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(1),  // 窗口大小1小时
                Time.minutes(2) // 滑动间隔2分钟
            ))
            .aggregate(new UserAuthorFeatureAggregator())
            .name("User-Author Feature Aggregation");

        // 打印聚合结果用于调试（采样）
        userFeatureStream
            .process(new ProcessFunction<UserFeatureAggregation, UserFeatureAggregation>() {
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
                public void processElement(UserFeatureAggregation value, Context ctx, Collector<UserFeatureAggregation> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[User Sample {}/{}] uid={} at {}: expose={}, 3s_views={}, like={}, follow={}",
                            sampleCount,
                            SAMPLE_COUNT,
                            value.uid,
                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
                            value.viewerExppostCnt1h,
                            value.viewer3sviewPostCnt1h,
                            value.viewerLikePostHis1h,
                            value.viewerFollowPostHis1h);
                    }
                    out.collect(value);
                }
            })
            .name("Debug User Sampling");

        userAuthorFeatureStream
            .process(new ProcessFunction<Tuple2<Long, Map<String, byte[]>>, Tuple2<Long, Map<String, byte[]>>>() {
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
                public void processElement(Tuple2<Long, Map<String, byte[]>> value, Context ctx, Collector<Tuple2<Long, Map<String, byte[]>>> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Author Sample {}/{}] uid={} at {}: authors={}, fields={}",
                            sampleCount,
                            SAMPLE_COUNT,
                            value.f0,
                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
                            value.f1.size(),
                            String.join(",", value.f1.keySet()));
                    }
                    out.collect(value);
                }
            })
            .name("Debug Author Sampling");

        // 5.1 用户特征写入Redis
        DataStream<Tuple2<String, byte[]>> userDataStream = userFeatureStream
            .map(new MapFunction<UserFeatureAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserFeatureAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.newBuilder()
                        .setViewerExppostCnt1H(agg.viewerExppostCnt1h)
                        .setViewerExp1PostCnt1H(agg.viewerExp1PostCnt1h)
                        .setViewerExp2PostCnt1H(agg.viewerExp2PostCnt1h)
                        .setViewer3SviewPostCnt1H(agg.viewer3sviewPostCnt1h)
                        .setViewer3Sview1PostCnt1H(agg.viewer3sview1PostCnt1h)
                        .setViewer3Sview2PostCnt1H(agg.viewer3sview2PostCnt1h)
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
            .name("User Feature to Protobuf Bytes");

        // 5.2 用户-作者特征写入Redis
        DataStream<Tuple2<String, Map<String, byte[]>>> authorDataStream = userAuthorFeatureStream
            .map(new MapFunction<Tuple2<Long, Map<String, byte[]>>, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(Tuple2<Long, Map<String, byte[]>> value) throws Exception {
                    String redisKey = AUTHOR_PREFIX + value.f0 + AUTHOR_SUFFIX;
                    return new Tuple2<>(redisKey, value.f1);
                }
            })
            .name("User-Author Feature to Protobuf Bytes");

        // 6. 写入Redis
        // 6.1 用户特征写入
        RedisConfig userConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        userConfig.setCommand("SET");
        userConfig.setTtl(600);
        RedisUtil.addRedisSink(
            userDataStream,
            userConfig,
            true,
            100
        );

        // 6.2 用户-作者特征写入（使用DEL_HMSET模式）
        RedisConfig authorConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        authorConfig.setCommand("DEL_HMSET");
        authorConfig.setTtl(600);
        RedisUtil.addRedisHashMapSink(
            authorDataStream,
            authorConfig,
            true,
            100
        );

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

    /**
     * 用户-作者特征聚合器
     */
    public static class UserAuthorFeatureAggregator implements AggregateFunction<UserFeatureEvent, UserAuthorFeature1hJob.UserAuthorAccumulator, Tuple2<Long, Map<String, byte[]>>> {
        @Override
        public UserAuthorFeature1hJob.UserAuthorAccumulator createAccumulator() {
            return new UserAuthorFeature1hJob.UserAuthorAccumulator();
        }

        @Override
        public UserAuthorFeature1hJob.UserAuthorAccumulator add(UserFeatureEvent event, UserAuthorFeature1hJob.UserAuthorAccumulator accumulator) {
            accumulator.uid = event.uid;
            accumulator.author = event.author;
            
            if ("expose".equals(event.eventType)) {
                accumulator.exposeRecTokens.add(event.recToken);
            }
            if ("view".equals(event.eventType)) {
                if (event.progressTime >= 3) accumulator.view3sRecTokens.add(event.recToken);
                if (event.progressTime >= 8) accumulator.view8sRecTokens.add(event.recToken);
                if (event.progressTime >= 12) accumulator.view12sRecTokens.add(event.recToken);
                if (event.progressTime >= 20) accumulator.view20sRecTokens.add(event.recToken);
                if (event.standingTime >= 5) accumulator.stand5sRecTokens.add(event.recToken);
                if (event.standingTime >= 10) accumulator.stand10sRecTokens.add(event.recToken);
                if (event.interaction != null) {
                    for (Integer it : event.interaction) {
                        if (it == 1) accumulator.likeRecTokens.add(event.recToken);
                    }
                }
            }
            
            return accumulator;
        }

        @Override
        public Tuple2<Long, Map<String, byte[]>> getResult(UserAuthorFeature1hJob.UserAuthorAccumulator accumulator) {
            Map<String, byte[]> authorFeatures = new HashMap<>();
            
            UserAuthorFeature1hJob.UserAuthorFeature1hAggregation authorResult = 
                new UserAuthorFeature1hJob.UserAuthorFeature1hAggregation();
            
            authorResult.uid = accumulator.uid;
            authorResult.author = accumulator.author;
            authorResult.userauthorExpCnt1h = accumulator.exposeRecTokens.size();
            authorResult.userauthor3sviewCnt1h = accumulator.view3sRecTokens.size();
            authorResult.userauthor8sviewCnt1h = accumulator.view8sRecTokens.size();
            authorResult.userauthor12sviewCnt1h = accumulator.view12sRecTokens.size();
            authorResult.userauthor20sviewCnt1h = accumulator.view20sRecTokens.size();
            authorResult.userauthor5sstandCnt1h = accumulator.stand5sRecTokens.size();
            authorResult.userauthor10sstandCnt1h = accumulator.stand10sRecTokens.size();
            authorResult.userauthorLikeCnt1h = accumulator.likeRecTokens.size();
            authorResult.updateTime = System.currentTimeMillis();

            byte[] value = RecFeature.RecUserAuthorFeature.newBuilder()
                .setUserauthorExpCnt1H(authorResult.userauthorExpCnt1h)
                .setUserauthor3SviewCnt1H(authorResult.userauthor3sviewCnt1h)
                .setUserauthor8SviewCnt1H(authorResult.userauthor8sviewCnt1h)
                .setUserauthor12SviewCnt1H(authorResult.userauthor12sviewCnt1h)
                .setUserauthor20SviewCnt1H(authorResult.userauthor20sviewCnt1h)
                .setUserauthor5SstandCnt1H(authorResult.userauthor5sstandCnt1h)
                .setUserauthor10SstandCnt1H(authorResult.userauthor10sstandCnt1h)
                .setUserauthorLikeCnt1H(authorResult.userauthorLikeCnt1h)
                .build()
                .toByteArray();

            authorFeatures.put(String.valueOf(accumulator.author), value);
            return new Tuple2<>(accumulator.uid, authorFeatures);
        }

        @Override
        public UserAuthorFeature1hJob.UserAuthorAccumulator merge(UserAuthorFeature1hJob.UserAuthorAccumulator a, UserAuthorFeature1hJob.UserAuthorAccumulator b) {
            a.exposeRecTokens.addAll(b.exposeRecTokens);
            a.view3sRecTokens.addAll(b.view3sRecTokens);
            a.view8sRecTokens.addAll(b.view8sRecTokens);
            a.view12sRecTokens.addAll(b.view12sRecTokens);
            a.view20sRecTokens.addAll(b.view20sRecTokens);
            a.stand5sRecTokens.addAll(b.stand5sRecTokens);
            a.stand10sRecTokens.addAll(b.stand10sRecTokens);
            a.likeRecTokens.addAll(b.likeRecTokens);
            return a;
        }
    }


} 