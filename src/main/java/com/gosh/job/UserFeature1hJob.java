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

public class UserFeature1hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeature1hJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:user_feature:{";
    private static String SUFFIX = "}:post1h";
    private static String AUTHOR_PREFIX = "rec:user_author_feature:{";
    private static String AUTHOR_SUFFIX = "}:post1h";
    private static final int MAX_EVENTS_PER_KEY = 100;

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
        SingleOutputStreamOperator<PostExposeEvent> exposeStream = filteredStream
            .flatMap(new ExposeEventParser())
            .name("Parse Expose Events");

        // 增加曝光事件计数日志（每分钟采样一次）
        exposeStream = exposeStream
            .process(new ProcessFunction<PostExposeEvent, PostExposeEvent>() {
                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
                private transient long lastSampleTime;
                private transient long counter;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0L;
                    counter = 0L;
                }

                @Override
                public void processElement(PostExposeEvent value, Context ctx, Collector<PostExposeEvent> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
                        if (lastSampleTime != 0L) {
                            LOG.info("[Expose Events] count in last minute: {}", counter);
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
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new ViewEventParser())
            .name("Parse View Events");

        // 增加观看事件计数日志（每分钟采样一次）
        viewStream = viewStream
            .process(new ProcessFunction<PostViewEvent, PostViewEvent>() {
                private static final long SAMPLE_INTERVAL = 60000; // 1分钟
                private transient long lastSampleTime;
                private transient long counter;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0L;
                    counter = 0L;
                }

                @Override
                public void processElement(PostViewEvent value, Context ctx, Collector<PostViewEvent> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime >= SAMPLE_INTERVAL) {
                        if (lastSampleTime != 0L) {
                            LOG.info("[View Events] count in last minute: {}", counter);
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
        DataStream<UserFeatureEvent> exposeFeatureStream = exposeStream
            .flatMap(new ExposeToFeatureMapper())
            .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> viewFeatureStream = viewStream
            .flatMap(new ViewToFeatureMapper())
            .name("View to Feature");

        // 3.5 合并两个流并进行特征处理
        DataStream<Tuple2<String, Object>> processedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            )
            .keyBy(event -> event.getUid())
            .process(new CombinedFeatureProcessor())
            .name("Combined Feature Processing");

        // 4. 分离用户特征和用户-作者特征流
        DataStream<UserFeatureAggregation> userFeatureStream = processedStream
            .filter(t -> "user".equals(t.f0))
            .map(new MapFunction<Tuple2<String, Object>, UserFeatureAggregation>() {
                @Override
                public UserFeatureAggregation map(Tuple2<String, Object> value) throws Exception {
                    return (UserFeatureAggregation) value.f1;
                }
            })
            .name("Extract User Features");

        DataStream<UserAuthorFeature1hJob.UserAuthorFeature1hAggregation> userAuthorFeatureStream = processedStream
            .filter(t -> "author".equals(t.f0))
            .map(new MapFunction<Tuple2<String, Object>, UserAuthorFeature1hJob.UserAuthorFeature1hAggregation>() {
                @Override
                public UserAuthorFeature1hJob.UserAuthorFeature1hAggregation map(Tuple2<String, Object> value) throws Exception {
                    return (UserAuthorFeature1hJob.UserAuthorFeature1hAggregation) value.f1;
                }
            })
            .name("Extract User-Author Features");

        // 5. 转换为Protobuf并写入Redis
        // 5.1 用户特征写入
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

        // 5.2 用户-作者特征写入
        DataStream<Tuple3<String, String, byte[]>> authorDataStream = userAuthorFeatureStream
            .map(new MapFunction<UserAuthorFeature1hJob.UserAuthorFeature1hAggregation, Tuple3<String, String, byte[]>>() {
                @Override
                public Tuple3<String, String, byte[]> map(UserAuthorFeature1hJob.UserAuthorFeature1hAggregation agg) throws Exception {
                    String redisKey = AUTHOR_PREFIX + agg.uid + AUTHOR_SUFFIX;
                    String field = String.valueOf(agg.author);

                    byte[] value = RecFeature.RecUserAuthorFeature.newBuilder()
                        .setUserauthorExpCnt1H(agg.userauthorExpCnt1h)
                        .setUserauthor3SviewCnt1H(agg.userauthor3sviewCnt1h)
                        .setUserauthor8SviewCnt1H(agg.userauthor8sviewCnt1h)
                        .setUserauthor12SviewCnt1H(agg.userauthor12sviewCnt1h)
                        .setUserauthor20SviewCnt1H(agg.userauthor20sviewCnt1h)
                        .setUserauthor5SstandCnt1H(agg.userauthor5sstandCnt1h)
                        .setUserauthor10SstandCnt1H(agg.userauthor10sstandCnt1h)
                        .setUserauthorLikeCnt1H(agg.userauthorLikeCnt1h)
                        .setExpireTime("1h")
                        .build()
                        .toByteArray();

                    return new Tuple3<>(redisKey, field, value);
                }
            })
            .name("User-Author Feature to Protobuf Bytes");

        // 打印聚合结果用于调试（采样）
        processedStream
            .process(new ProcessFunction<Tuple2<String, Object>, Tuple2<String, Object>>() {
                private static final long SAMPLE_INTERVAL = 60000; // 采样间隔1分钟
                private static final int SAMPLE_COUNT = 3; // 每次采样3条
                private transient long lastSampleTime;
                private transient Map<String, Integer> typeSampleCount; // 每种类型的采样计数

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastSampleTime = 0;
                    typeSampleCount = new HashMap<>();
                }

                @Override
                public void processElement(Tuple2<String, Object> value, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception {
                    // 采样日志
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL); // 对齐到分钟
                        typeSampleCount.clear();
                    }

                    String type = value.f0;
                    int currentCount = typeSampleCount.getOrDefault(type, 0);

                    // 每种类型只采样前3条
                    if (currentCount < SAMPLE_COUNT) {
                        typeSampleCount.put(type, currentCount + 1);
                        if ("user".equals(type)) {
                            UserFeatureAggregation userFeature = (UserFeatureAggregation) value.f1;
                            LOG.info("[User Sample {}/{}] uid={} at {}: expose={}, 3s_views={}, like={}, follow={}",
                                currentCount + 1,
                                SAMPLE_COUNT,
                                userFeature.uid,
                                new SimpleDateFormat("HH:mm:ss").format(new Date(userFeature.updateTime)),
                                userFeature.viewerExppostCnt1h,
                                userFeature.viewer3sviewPostCnt1h,
                                userFeature.viewerLikePostHis1h,
                                userFeature.viewerFollowPostHis1h);
                        } else if ("author".equals(type)) {
                            UserAuthorFeature1hJob.UserAuthorFeature1hAggregation authorFeature = 
                                (UserAuthorFeature1hJob.UserAuthorFeature1hAggregation) value.f1;
                            LOG.info("[Author Sample {}/{}] uid={} author={} at {}: expose={}, 3s_views={}, like={}",
                                currentCount + 1,
                                SAMPLE_COUNT,
                                authorFeature.uid,
                                authorFeature.author,
                                new SimpleDateFormat("HH:mm:ss").format(new Date(authorFeature.updateTime)),
                                authorFeature.userauthorExpCnt1h,
                                authorFeature.userauthor3sviewCnt1h,
                                authorFeature.userauthorLikeCnt1h);
                        }
                    }
                    out.collect(value);
                }
            })
            .name("Debug Sampling");

        // 5.1 用户特征写入Redis
        RedisConfig userConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        userConfig.setCommand("SET");
        userConfig.setTtl(600);
        RedisUtil.addRedisSink(
            userDataStream,
            userConfig,
            true,
            100
        );

        // 5.2 用户-作者特征写入Redis（使用DEL_HSET模式，原子操作）
        RedisConfig authorConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        authorConfig.setCommand("DEL_HSET");
        authorConfig.setTtl(600);
        RedisUtil.addRedisHashSink(
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
     * 简单计数器限制函数 - 每个用户维护一个计数器
     * 超过限制就丢弃事件，简单直接
     */
    public static class SimpleCounterLimitFunction extends ProcessFunction<UserFeatureEvent, UserFeatureEvent> {
        private final int maxEventsPerWindow = 100;
        private final long windowSizeMs = 300000L; // 5分钟窗口
        private final long cleanupInterval = 300000L; // 5分钟清理一次
        
        private transient Map<String, Integer> userCounters; // key: uid_windowStart, value: count
        private transient long lastCleanupTime;

        @Override
        public void open(Configuration parameters) {
            userCounters = new HashMap<>();
            lastCleanupTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(UserFeatureEvent event, Context ctx, Collector<UserFeatureEvent> out) {
            long windowStart = (event.timestamp / windowSizeMs) * windowSizeMs;
            String userWindowKey = event.uid + "_" + windowStart;
            
            // 定期清理过期的计数器
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCleanupTime > cleanupInterval) {
                cleanupExpiredCounters(windowStart);
                lastCleanupTime = currentTime;
            }
            
            // 获取当前计数
            int currentCount = userCounters.getOrDefault(userWindowKey, 0);
            
            // 如果未超过限制，接受事件并增加计数
            if (currentCount < maxEventsPerWindow) {
                userCounters.put(userWindowKey, currentCount + 1);
                out.collect(event);
            }
            // 超过限制的事件直接丢弃
        }
        
        private void cleanupExpiredCounters(long currentWindowStart) {
            // 清理超过2个窗口的旧计数器
            long cutoffWindow = currentWindowStart - (2 * windowSizeMs);
            userCounters.entrySet().removeIf(entry -> {
                String key = entry.getKey();
                long windowStart = Long.parseLong(key.substring(key.lastIndexOf('_') + 1));
                return windowStart < cutoffWindow;
            });
        }

    }

    /**
     * 合并用户特征和用户-作者特征的处理函数
     * 同时计算两种特征，共用事件计数限制
     */
    public static class CombinedFeatureProcessor extends ProcessFunction<UserFeatureEvent, Tuple2<String, Object>> {
        private final int maxEventsPerWindow = 100;
        private final long windowSizeMs = 3600000L; // 1小时窗口
        private final long slidingMs = 120000L; // 2分钟滑动
        private final long cleanupInterval = 300000L; // 5分钟清理一次
        
        private transient Map<String, WindowState> windowStates; // key: uid_windowStart
        private transient long lastCleanupTime;

        private static class WindowState {
            int eventCount;
            UserFeatureAccumulator userAccumulator;
            Map<Long, UserAuthorFeature1hJob.UserAuthorAccumulator> authorAccumulators; // key: authorId
            long windowStart;
            
            WindowState(long windowStart) {
                this.eventCount = 0;
                this.userAccumulator = new UserFeatureAccumulator();
                this.authorAccumulators = new HashMap<>();
                this.windowStart = windowStart;
            }
        }

        @Override
        public void open(Configuration parameters) {
            windowStates = new HashMap<>();
            lastCleanupTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(UserFeatureEvent event, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception {
            long currentTime = event.timestamp;
            
            // 定期清理过期的窗口
            if (currentTime - lastCleanupTime > cleanupInterval) {
                cleanupExpiredWindows(currentTime);
                lastCleanupTime = currentTime;
            }
            
            // 找到所有包含当前事件的窗口
            long firstWindowStart = (currentTime / slidingMs) * slidingMs;
            for (long windowStart = firstWindowStart; windowStart > firstWindowStart - windowSizeMs; windowStart -= slidingMs) {
                processEventForWindow(event, windowStart, currentTime, out);
            }
        }

        private void processEventForWindow(UserFeatureEvent event, final long windowStart, final long currentTime, Collector<Tuple2<String, Object>> out) {
            String windowKey = event.uid + "_" + windowStart;
            WindowState state = windowStates.computeIfAbsent(windowKey, k -> new WindowState(windowStart));
            
            // 如果未超过限制，处理事件
            if (state.eventCount < maxEventsPerWindow) {
                state.eventCount++;
                
                // 更新用户特征累加器
                UserFeatureCommon.addEventToAccumulator(event, state.userAccumulator);
                
                // 更新用户-作者特征累加器
                if (event.author > 0) {
                    UserAuthorFeature1hJob.UserAuthorAccumulator authorAccumulator = 
                        state.authorAccumulators.computeIfAbsent(event.author, 
                            k -> new UserAuthorFeature1hJob.UserAuthorAccumulator());
                    authorAccumulator.uid = event.uid;
                    authorAccumulator.author = event.author;
                    
                    if ("expose".equals(event.eventType)) {
                        authorAccumulator.exposeRecTokens.add(event.recToken);
                    }
                    if ("view".equals(event.eventType)) {
                        if (event.progressTime >= 3) authorAccumulator.view3sRecTokens.add(event.recToken);
                        if (event.progressTime >= 8) authorAccumulator.view8sRecTokens.add(event.recToken);
                        if (event.progressTime >= 12) authorAccumulator.view12sRecTokens.add(event.recToken);
                        if (event.progressTime >= 20) authorAccumulator.view20sRecTokens.add(event.recToken);
                        if (event.standingTime >= 5) authorAccumulator.stand5sRecTokens.add(event.recToken);
                        if (event.standingTime >= 10) authorAccumulator.stand10sRecTokens.add(event.recToken);
                        if (event.interaction != null) {
                            for (Integer it : event.interaction) {
                                if (it == 1) authorAccumulator.likeRecTokens.add(event.recToken);
                            }
                        }
                    }
                }
                
                // 如果到达窗口结束时间，输出结果
                if (currentTime >= windowStart + windowSizeMs) {
                    // 1. 输出用户特征
                    UserFeatureAggregation userResult = new UserFeatureAggregation();
                    userResult.uid = event.uid;
                    userResult.viewerExppostCnt1h = state.userAccumulator.exposePostIds.size();
                    userResult.viewerExp1PostCnt1h = 0;
                    userResult.viewerExp2PostCnt1h = 0;
                    userResult.viewer3sviewPostCnt1h = state.userAccumulator.view3sPostIds.size();
                    userResult.viewer3sview1PostCnt1h = state.userAccumulator.view3s1PostIds.size();
                    userResult.viewer3sview2PostCnt1h = state.userAccumulator.view3s2PostIds.size();
                    userResult.viewer3sviewPostHis1h = UserFeatureCommon.buildPostHistoryString(state.userAccumulator.view3sPostDetails, 10);
                    userResult.viewer5sstandPostHis1h = UserFeatureCommon.buildPostHistoryString(state.userAccumulator.stand5sPostDetails, 10);
                    userResult.viewerLikePostHis1h = UserFeatureCommon.buildPostListString(state.userAccumulator.likePostIds, 10);
                    userResult.viewerFollowPostHis1h = UserFeatureCommon.buildPostListString(state.userAccumulator.followPostIds, 10);
                    userResult.viewerProfilePostHis1h = UserFeatureCommon.buildPostListString(state.userAccumulator.profilePostIds, 10);
                    userResult.viewerPosinterPostHis1h = UserFeatureCommon.buildPostListString(state.userAccumulator.posinterPostIds, 10);
                    userResult.updateTime = currentTime;
                    
                    out.collect(new Tuple2<>("user", userResult));
                    
                    // 2. 输出用户-作者特征
                    for (Map.Entry<Long, UserAuthorFeature1hJob.UserAuthorAccumulator> entry : state.authorAccumulators.entrySet()) {
                        UserAuthorFeature1hJob.UserAuthorFeature1hAggregation authorResult = 
                            new UserAuthorFeature1hJob.UserAuthorFeature1hAggregation();
                        UserAuthorFeature1hJob.UserAuthorAccumulator acc = entry.getValue();
                        
                        authorResult.uid = acc.uid;
                        authorResult.author = acc.author;
                        authorResult.userauthorExpCnt1h = acc.exposeRecTokens.size();
                        authorResult.userauthor3sviewCnt1h = acc.view3sRecTokens.size();
                        authorResult.userauthor8sviewCnt1h = acc.view8sRecTokens.size();
                        authorResult.userauthor12sviewCnt1h = acc.view12sRecTokens.size();
                        authorResult.userauthor20sviewCnt1h = acc.view20sRecTokens.size();
                        authorResult.userauthor5sstandCnt1h = acc.stand5sRecTokens.size();
                        authorResult.userauthor10sstandCnt1h = acc.stand10sRecTokens.size();
                        authorResult.userauthorLikeCnt1h = acc.likeRecTokens.size();
                        authorResult.updateTime = currentTime;
                        
                        out.collect(new Tuple2<>("author", authorResult));
                    }
                    
                    // 移除已完成的窗口
                    windowStates.remove(windowKey);
                }
            }
        }
        
        private void cleanupExpiredWindows(long currentTime) {
            final long cutoffTime = currentTime - (2 * windowSizeMs);
            windowStates.entrySet().removeIf(entry -> entry.getValue().windowStart < cutoffTime);
        }
    }

    /**
     * 合并计数限制和时间窗口聚合的处理函数
     */
    public static class CombinedLimitAndAggregateFunction extends ProcessFunction<UserFeatureEvent, UserFeatureAggregation> {
        private final int maxEventsPerWindow = 100;
        private final long windowSizeMs = 3600000L; // 1小时窗口
        private final long slidingMs = 120000L; // 2分钟滑动
        private final long cleanupInterval = 300000L; // 5分钟清理一次
        
        private transient Map<String, UserWindowState> userWindows; // key: uid_windowStart
        private transient long lastCleanupTime;

        private static class UserWindowState {
            int eventCount;
            UserFeatureAccumulator accumulator;
            long windowStart;
            
            UserWindowState(long windowStart) {
                this.eventCount = 0;
                this.accumulator = new UserFeatureAccumulator();
                this.windowStart = windowStart;
            }
        }

        @Override
        public void open(Configuration parameters) {
            userWindows = new HashMap<>();
            lastCleanupTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(UserFeatureEvent event, Context ctx, Collector<UserFeatureAggregation> out) throws Exception {
            long currentTime = event.timestamp;
            
            // 定期清理过期的窗口
            if (currentTime - lastCleanupTime > cleanupInterval) {
                cleanupExpiredWindows(currentTime);
                lastCleanupTime = currentTime;
            }
            
            // 找到所有包含当前事件的窗口
            long firstWindowStart = (currentTime / slidingMs) * slidingMs;
            for (long windowStart = firstWindowStart; windowStart > firstWindowStart - windowSizeMs; windowStart -= slidingMs) {
                processEventForWindow(event, windowStart, currentTime, out);
            }
        }

        private void processEventForWindow(UserFeatureEvent event, final long windowStart, final long currentTime, Collector<UserFeatureAggregation> out) {
            String windowKey = event.uid + "_" + windowStart;
            UserWindowState state = userWindows.computeIfAbsent(windowKey, k -> new UserWindowState(windowStart));
            
            // 如果未超过限制，处理事件
            if (state.eventCount < maxEventsPerWindow) {
                state.eventCount++;
                UserFeatureCommon.addEventToAccumulator(event, state.accumulator);
                
                // 如果到达窗口结束时间，输出结果
                if (currentTime >= windowStart + windowSizeMs) {
                    UserFeatureAggregation result = new UserFeatureAggregation();
                    result.uid = event.uid;
                    
                    // 设置聚合结果
                    result.viewerExppostCnt1h = state.accumulator.exposePostIds.size();
                    result.viewerExp1PostCnt1h = 0; // 暂时无法统计图片和视频的单独曝光数
                    result.viewerExp2PostCnt1h = 0;
                    result.viewer3sviewPostCnt1h = state.accumulator.view3sPostIds.size();
                    result.viewer3sview1PostCnt1h = state.accumulator.view3s1PostIds.size();
                    result.viewer3sview2PostCnt1h = state.accumulator.view3s2PostIds.size();
                    result.viewer3sviewPostHis1h = UserFeatureCommon.buildPostHistoryString(state.accumulator.view3sPostDetails, 10);
                    result.viewer5sstandPostHis1h = UserFeatureCommon.buildPostHistoryString(state.accumulator.stand5sPostDetails, 10);
                    result.viewerLikePostHis1h = UserFeatureCommon.buildPostListString(state.accumulator.likePostIds, 10);
                    result.viewerFollowPostHis1h = UserFeatureCommon.buildPostListString(state.accumulator.followPostIds, 10);
                    result.viewerProfilePostHis1h = UserFeatureCommon.buildPostListString(state.accumulator.profilePostIds, 10);
                    result.viewerPosinterPostHis1h = UserFeatureCommon.buildPostListString(state.accumulator.posinterPostIds, 10);
                    result.updateTime = currentTime;
                    
                    out.collect(result);
                    
                    // 移除已完成的窗口
                    userWindows.remove(windowKey);
                }
            }
        }
        
        private void cleanupExpiredWindows(long currentTime) {
            // 清理超过1个完整窗口时间的旧数据
            final long cutoffTime = currentTime - (2 * windowSizeMs);
            userWindows.entrySet().removeIf(entry -> entry.getValue().windowStart < cutoffTime);
        }
    }

    /**
     * 用户特征聚合器
     */
    public static class UserFeatureAggregator implements AggregateFunction<UserFeatureEvent, UserFeatureAccumulator, UserFeatureAggregation> {
        @Override
        public UserFeatureAccumulator createAccumulator() {
            return new UserFeatureAccumulator();
        }

        @Override
        public UserFeatureAccumulator add(UserFeatureEvent event, UserFeatureAccumulator accumulator) {
            return UserFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public UserFeatureAggregation getResult(UserFeatureAccumulator accumulator) {
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
        public UserFeatureAccumulator merge(UserFeatureAccumulator a, UserFeatureAccumulator b) {
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