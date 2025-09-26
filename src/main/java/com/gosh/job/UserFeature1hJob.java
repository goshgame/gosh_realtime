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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
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

        // 3.5 合并两个流，限制数据量并直接在ProcessFunction中实现时间窗口聚合
        DataStream<Tuple2<String, Object>> processedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureCommon.UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            )
            .keyBy(event -> event.getUid())
            .process(new KeyedProcessFunction<Long, UserFeatureCommon.UserFeatureEvent, Tuple2<String, Object>>() {
                private final int maxEventsPerWindow = 100;
                private final long windowSizeMs = 3600000L; // 1小时窗口
                private final long slidingMs = 120000L; // 2分钟滑动
                private final long cleanupInterval = 300000L; // 5分钟清理一次
                
                private transient Map<String, WindowState> windowStates; // key: uid_windowStart
                private transient long lastCleanupTime;
                private transient int subtaskIndex;
                private transient int numberOfParallelSubtasks;

                @Override
                public void open(Configuration parameters) {
                    windowStates = new HashMap<>();
                    lastCleanupTime = System.currentTimeMillis();
                    subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                    numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    LOG.info("Process function opened: subtask {}/{}", subtaskIndex + 1, numberOfParallelSubtasks);
                }

                @Override
                public void processElement(UserFeatureCommon.UserFeatureEvent event, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception {
                    // 检查事件时间戳是否是秒级的
                    long currentTime = event.timestamp;
                    if (currentTime < 1000000000000L) { // 如果是秒级时间戳
                        currentTime = currentTime * 1000; // 转换为毫秒级
                    }
                    
                    // 定期清理过期的窗口
                    if (currentTime - lastCleanupTime > cleanupInterval) {
                        cleanupExpiredWindows(currentTime);
                        lastCleanupTime = currentTime;
                    }
                    
                    // 找到所有包含当前事件的窗口
                    long firstWindowStart = (currentTime / slidingMs) * slidingMs;
                    
                    // 调试日志：事件时间和窗口信息
                    LOG.error("[Debug] Processing event: uid={}, event_time={}, current_time={}, firstWindow={}, type={}",
                        event.uid,
                        new SimpleDateFormat("HH:mm:ss").format(new Date(event.timestamp)),
                        new SimpleDateFormat("HH:mm:ss").format(new Date(currentTime)),
                        new SimpleDateFormat("HH:mm:ss").format(new Date(firstWindowStart)),
                        event.eventType);
                    
                    // 为每个相关的窗口注册定时器
                    for (long windowStart = firstWindowStart; windowStart > firstWindowStart - windowSizeMs; windowStart -= slidingMs) {
                        // 注册窗口结束时的定时器
                        long windowEnd = windowStart + windowSizeMs;
                        ctx.timerService().registerProcessingTimeTimer(windowEnd);
                        
                        processEventForWindow(event, windowStart, currentTime, out);
                    }
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Object>> out) throws Exception {
                    // 找到这个时间戳对应的窗口开始时间
                    long windowStart = timestamp - windowSizeMs;
                    String key = ctx.getCurrentKey() + "_" + windowStart;
                    
                    WindowState state = windowStates.get(key);
                    if (state != null) {
                        LOG.error("[Debug] Timer triggered: uid={}, start={}, end={}, events={}",
                            ctx.getCurrentKey(),
                            new SimpleDateFormat("HH:mm:ss").format(new Date(windowStart)),
                            new SimpleDateFormat("HH:mm:ss").format(new Date(timestamp)),
                            state.eventCount);
                            
                        // 1. 输出用户特征
                        UserFeatureAggregation userResult = new UserFeatureAggregation();
                        userResult.uid = ctx.getCurrentKey();
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
                        userResult.updateTime = timestamp;
                        
                        if (state.eventCount >= 10) {
                            LOG.info("[Window] uid={}, expose={}, 3s_views={}, events={}, subtask={}/{}",
                                userResult.uid,
                                userResult.viewerExppostCnt1h,
                                userResult.viewer3sviewPostCnt1h,
                                state.eventCount,
                                subtaskIndex + 1,
                                numberOfParallelSubtasks);
                        }
                        
                        out.collect(new Tuple2<>("user", userResult));
                        
                        // 2. 输出用户-作者特征
                        Map<String, byte[]> authorFeatures = new HashMap<>();
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
                            authorResult.updateTime = timestamp;

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

                            authorFeatures.put(String.valueOf(acc.author), value);
                        }
                        
                        if (!authorFeatures.isEmpty() && state.eventCount >= 10) {
                            LOG.info("[Window] uid={}, authors={}, events={}, subtask={}/{}",
                                userResult.uid,
                                authorFeatures.size(),
                                state.eventCount,
                                subtaskIndex + 1,
                                numberOfParallelSubtasks);
                        }
                        
                        if (!authorFeatures.isEmpty()) {
                            out.collect(new Tuple2<>("author", new Tuple2<>(userResult.uid, authorFeatures)));
                        }
                        
                        // 移除已完成的窗口
                        windowStates.remove(key);
                    }
                }

                private void processEventForWindow(UserFeatureCommon.UserFeatureEvent event, final long windowStart, final long currentTime, Collector<Tuple2<String, Object>> out) {
                    String windowKey = event.uid + "_" + windowStart;
                    WindowState state = windowStates.computeIfAbsent(windowKey, k -> {
                        LOG.error("[Debug] New window: uid={}, start={}, current_time={}, subtask={}/{}",
                            event.uid,
                            new SimpleDateFormat("HH:mm:ss").format(new Date(windowStart)),
                            new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis())),
                            subtaskIndex + 1,
                            numberOfParallelSubtasks);
                        return new WindowState(windowStart);
                    });
                    
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
                            LOG.error("[Debug] Window complete: uid={}, start={}, end={}, events={}, subtask={}/{}",
                                event.uid,
                                new SimpleDateFormat("HH:mm:ss").format(new Date(windowStart)),
                                new SimpleDateFormat("HH:mm:ss").format(new Date(currentTime)),
                                state.eventCount,
                                subtaskIndex + 1,
                                numberOfParallelSubtasks);
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
                            
                            // 只打印有意义的聚合结果
                            if (state.eventCount >= 10) {
                                LOG.info("[Window] uid={}, expose={}, 3s_views={}, events={}, subtask={}/{}",
                                    event.uid,
                                    userResult.viewerExppostCnt1h,
                                    userResult.viewer3sviewPostCnt1h,
                                    state.eventCount,
                                    subtaskIndex + 1,
                                    numberOfParallelSubtasks);
                            }
                            
                            out.collect(new Tuple2<>("user", userResult));
                            
                            // 2. 输出用户-作者特征
                            Map<String, byte[]> authorFeatures = new HashMap<>();
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

                                authorFeatures.put(String.valueOf(acc.author), value);
                            }
                            
                            if (!authorFeatures.isEmpty() && state.eventCount >= 10) {
                                LOG.info("[Window] uid={}, authors={}, events={}, subtask={}/{}",
                                    event.uid,
                                    authorFeatures.size(),
                                    state.eventCount,
                                    subtaskIndex + 1,
                                    numberOfParallelSubtasks);
                            }
                            
                            if (!authorFeatures.isEmpty()) {
                                out.collect(new Tuple2<>("author", new Tuple2<>(event.uid, authorFeatures)));
                            }
                            
                            // 移除已完成的窗口
                            windowStates.remove(windowKey);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("[Limit] uid={}, events={}, subtask={}/{}",
                                event.uid,
                                state.eventCount,
                                subtaskIndex + 1,
                                numberOfParallelSubtasks);
                        }
                    }
                }
                
                private void cleanupExpiredWindows(long currentTime) {
                    final long cutoffTime = currentTime - (2 * windowSizeMs);
                    int removedCount = 0;
                    for (Iterator<Map.Entry<String, WindowState>> it = windowStates.entrySet().iterator(); it.hasNext();) {
                        Map.Entry<String, WindowState> entry = it.next();
                        if (entry.getValue().windowStart < cutoffTime) {
                            it.remove();
                            removedCount++;
                        }
                    }
                    if (removedCount > 0) {
                        LOG.info("[Cleanup] removed={}, subtask={}/{}",
                            removedCount,
                            subtaskIndex + 1,
                            numberOfParallelSubtasks);
                    }
                }
            })
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

        DataStream<Tuple2<Long, Map<String, byte[]>>> userAuthorFeatureStream = processedStream
            .filter(t -> "author".equals(t.f0))
            .map(new MapFunction<Tuple2<String, Object>, Tuple2<Long, Map<String, byte[]>>>() {
                @Override
                public Tuple2<Long, Map<String, byte[]>> map(Tuple2<String, Object> value) throws Exception {
                    @SuppressWarnings("unchecked")
                    Tuple2<Long, Map<String, byte[]>> result = (Tuple2<Long, Map<String, byte[]>>) value.f1;
                    return result;
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

        // 5.2 用户-作者特征写入（每个窗口一次性写入所有fields）
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

        // 6.2 用户-作者特征写入（使用HMSET模式，先删除再批量写入）
        RedisConfig authorConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        authorConfig.setCommand("DEL_HMSET");
        authorConfig.setTtl(600);
        RedisUtil.addRedisHashMapSink(
            authorDataStream,
            authorConfig,
            true,
            100
        );

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
                    LOG.info("Sample processor opened");
                }

                @Override
                public void processElement(Tuple2<String, Object> value, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception {
                    // 采样日志
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL); // 对齐到分钟
                        typeSampleCount.clear();
                        LOG.info("Sample interval reset");
                    }

                    String type = value.f0;
                    int currentCount = typeSampleCount.getOrDefault(type, 0);

                    // 每种类型只采样前3条
                    if (currentCount < SAMPLE_COUNT) {
                        typeSampleCount.put(type, currentCount + 1);
                        if ("user".equals(type)) {
                            UserFeatureAggregation userFeature = (UserFeatureAggregation) value.f1;
                            LOG.info("[Sample] user={}, expose={}, 3s_views={}, like={}, follow={}",
                                userFeature.uid,
                                userFeature.viewerExppostCnt1h,
                                userFeature.viewer3sviewPostCnt1h,
                                userFeature.viewerLikePostHis1h,
                                userFeature.viewerFollowPostHis1h);
                        } else if ("author".equals(type)) {
                            @SuppressWarnings("unchecked")
                            Tuple2<Long, Map<String, byte[]>> authorFeature = (Tuple2<Long, Map<String, byte[]>>) value.f1;
                            LOG.info("[Sample] user={}, authors={}, fields={}",
                                authorFeature.f0,
                                authorFeature.f1.size(),
                                String.join(",", authorFeature.f1.keySet()));
                        }
                    }
                    out.collect(value);
                }
            })
            .setParallelism(1) // 采样处理器只需要一个并行度
            .disableChaining() // 禁用链接，独立运行
            .name("Debug Sampling");

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


} 