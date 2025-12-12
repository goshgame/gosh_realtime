package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class PostRecFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(PostRecFeature24hJob.class);
    
    // Redis key前缀
    private static final String USER_PREFIX = "rec:user_feature:{";
    private static final String USER_SUFFIX = "}:post24h";
    private static final String POST_PREFIX = "rec:item_feature:{";
    private static final String POST_SUFFIX = "}:post24h";
    private static final String USER_AUTHOR_PREFIX = "rec:user_author_feature:{";
    private static final String USER_AUTHOR_SUFFIX = "}:post24h";
    
    // 事件类型常量
    private static final String EVENT_POST_EXPOSURE = "postExposure";
    private static final String EVENT_VIDEO_PLAY_DURATION = "videoPlayDuration";
    private static final String EVENT_IMAGE_PLAY = "imagePlay";
    private static final String EVENT_VIDEO_PLAY_COMPLETE = "videoPlayComplete";
    private static final String EVENT_POST_LIKE = "postLikeButtonClicked";
    private static final String EVENT_POST_COMMENT = "postCommentSucceed";
    private static final String EVENT_POST_SHARE = "postShare";
    private static final String EVENT_POST_FOLLOW = "postFollow";
    private static final String EVENT_COLLECT = "collect_page_click";
    private static final String EVENT_POST_CLICK_NICKNAME = "postClickNickname";
    private static final String EVENT_POST_SWIPE_RIGHT = "postSwipeRight";
    private static final String EVENT_POST_CLICK_AVATAR = "postClickAvatar";
    private static final String EVENT_POST_REPORT = "postReportClick";
    private static final String EVENT_POST_UNINTERESTED = "postUninterestedClick";
    private static final String EVENT_POST_PAID = "post_paid_unlock_entry_show";
    
    // SessionWindow gap: 1分钟
    private static final long SESSION_GAP_MINUTES = 1;
    
    // 滑动窗口：24小时窗口，10分钟更新
    private static final long WINDOW_SIZE_HOURS = 24;
    private static final long SLIDE_INTERVAL_MINUTES = 10;
    // 单用户/用户作者窗口内事件上限
    private static final int MAX_EVENTS_PER_USER_WINDOW = 300;

    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Starting PostRecFeature24hJob");
        LOG.info("Processing post events from advertise topic");
        LOG.info("SessionWindow gap: {} minutes", SESSION_GAP_MINUTES);
        LOG.info("SlidingWindow: {} hours, slide: {} minutes", WINDOW_SIZE_HOURS, SLIDE_INTERVAL_MINUTES);
        LOG.info("========================================");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        LOG.info("Job parallelism set to: {}", env.getParallelism());
        
        env.getConfig().setAutoWatermarkInterval(1000);

        // 第二步：创建Source，Kafka环境（topic=advertise）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "advertise"
        );

        // 第三步：使用KafkaSource创建DataStream
        // 不在这里设置水位线，避免对所有消息进行JSON解析，提升性能
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)");

        // 3.1 解析所有post相关事件
        // 在解析后的事件上设置水位线，只对过滤和解析后的事件进行时间戳提取
        SingleOutputStreamOperator<PostEvent> eventStream = filteredStream
            .flatMap(new PostEventParser())
            .name("Parse Post Events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<PostEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, recordTimestamp) -> event.eventTime)
            );

        // 事件采样日志（每分钟最多3条）
        eventStream = eventStream
            .process(new ProcessFunction<PostEvent, PostEvent>() {
                private static final long SAMPLE_INTERVAL = 60_000L;
                private static final int SAMPLE_COUNT = 3;
                private transient long lastSampleTime;
                private transient int sampleCount;

                @Override
                public void open(Configuration parameters) {
                    lastSampleTime = 0;
                    sampleCount = 0;
                }

                @Override
                public void processElement(PostEvent value, Context ctx, Collector<PostEvent> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Sample {}/{}] eventType={} uid={} postId={} authorId={} recToken={} exposedPos={} eventTime={}",
                            sampleCount, SAMPLE_COUNT, value.eventType, value.uid, value.postId, value.authorId,
                            value.recToken, value.exposedPos, value.eventTime);
                    }
                    out.collect(value);
                }
            })
            .name("Sample Post Events");

        // 3.2 SessionWindow聚合：基于rec_token + post_id
        SingleOutputStreamOperator<SessionSummary> sessionStream = eventStream
            .keyBy(new KeySelector<PostEvent, String>() {
                @Override
                public String getKey(PostEvent value) throws Exception {
                    return value.recToken + "|" + value.postId;
                }
            })
            .window(EventTimeSessionWindows.withGap(org.apache.flink.streaming.api.windowing.time.Time.minutes(SESSION_GAP_MINUTES)))
            .aggregate(new SessionAggregator(), new SessionWindowFunction())
            .name("Session Window Aggregation");

        // 第四步：分别计算三类特征
        // 4.1 User侧特征（单阶段，限制单用户窗口内事件数）
        DataStream<UserFeature24hAggregation> userFeatureStream = sessionStream
            .keyBy((KeySelector<SessionSummary, Long>) value -> value.uid)
            .window(SlidingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.hours(WINDOW_SIZE_HOURS),
                org.apache.flink.streaming.api.windowing.time.Time.minutes(SLIDE_INTERVAL_MINUTES)
            ))
            .aggregate(new UserFeatureAggregator())
            .name("User Feature Aggregation");

        // 4.2 Post侧特征
        DataStream<PostFeature24hAggregation> postFeatureStream = sessionStream
            .keyBy(new KeySelector<SessionSummary, Long>() {
                @Override
                public Long getKey(SessionSummary value) throws Exception {
                    return value.postId;
                }
            })
            .window(SlidingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.hours(WINDOW_SIZE_HOURS),
                org.apache.flink.streaming.api.windowing.time.Time.minutes(SLIDE_INTERVAL_MINUTES)
            ))
            .aggregate(new PostFeatureAggregator())
            .name("Post Feature Aggregation");

        // 4.3 UserAuthor侧特征（单阶段，限制单用户窗口内事件数，按uid聚合成hset）
        DataStream<UserAuthorFeatureMapAggregation> userAuthorFeatureStream = sessionStream
            .keyBy((KeySelector<SessionSummary, Long>) value -> value.uid)
            .window(SlidingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.hours(WINDOW_SIZE_HOURS),
                org.apache.flink.streaming.api.windowing.time.Time.minutes(SLIDE_INTERVAL_MINUTES)
            ))
            .aggregate(new UserAuthorMapAggregator())
            .name("UserAuthor Feature Aggregation");

        // 第五步：转换为Protobuf并写入Redis
        // 5.1 User特征
        DataStream<Tuple2<String, byte[]>> userDataStream = userFeatureStream
            .map(new MapFunction<UserFeature24hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserFeature24hAggregation agg) throws Exception {
                    String redisKey = USER_PREFIX + agg.uid + USER_SUFFIX;
                    byte[] value = buildUserFeatureProto(agg);
                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("User Feature to Protobuf");

        // User特征写入前采样日志
        userDataStream = userDataStream
            .process(new ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>() {
                private static final long SAMPLE_INTERVAL = 60_000L;
                private static final int SAMPLE_COUNT = 3;
                private transient long lastSampleTime;
                private transient int sampleCount;

                @Override
                public void open(Configuration parameters) {
                    lastSampleTime = 0;
                    sampleCount = 0;
                }

                @Override
                public void processElement(Tuple2<String, byte[]> value, Context ctx, Collector<Tuple2<String, byte[]>> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Sample {}/{}] Redis UserFeature key={} bytes={}", sampleCount, SAMPLE_COUNT, value.f0, value.f1 != null ? value.f1.length : 0);
                    }
                    out.collect(value);
                }
            })
            .name("Sample User Feature Output");

        // 5.2 Post特征
        DataStream<Tuple2<String, byte[]>> postDataStream = postFeatureStream
            .map(new MapFunction<PostFeature24hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(PostFeature24hAggregation agg) throws Exception {
                    String redisKey = POST_PREFIX + agg.postId + POST_SUFFIX;
                    byte[] value = buildPostFeatureProto(agg);
                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("Post Feature to Protobuf");

        // Post特征写入前采样日志
        postDataStream = postDataStream
            .process(new ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>() {
                private static final long SAMPLE_INTERVAL = 60_000L;
                private static final int SAMPLE_COUNT = 3;
                private transient long lastSampleTime;
                private transient int sampleCount;

                @Override
                public void open(Configuration parameters) {
                    lastSampleTime = 0;
                    sampleCount = 0;
                }

                @Override
                public void processElement(Tuple2<String, byte[]> value, Context ctx, Collector<Tuple2<String, byte[]>> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        LOG.info("[Sample {}/{}] Redis PostFeature key={} bytes={}", sampleCount, SAMPLE_COUNT, value.f0, value.f1 != null ? value.f1.length : 0);
                    }
                    out.collect(value);
                }
            })
            .name("Sample Post Feature Output");

        // 5.3 UserAuthor特征
        DataStream<Tuple2<String, Map<String, byte[]>>> userAuthorDataStream = userAuthorFeatureStream
            .filter(agg -> agg != null && agg.authorFeatures != null && !agg.authorFeatures.isEmpty())
            .map(new MapFunction<UserAuthorFeatureMapAggregation, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(UserAuthorFeatureMapAggregation agg) throws Exception {
                    String redisKey = USER_AUTHOR_PREFIX + agg.uid + USER_AUTHOR_SUFFIX;
                    return new Tuple2<>(redisKey, agg.authorFeatures);
                }
            })
            .name("UserAuthor Feature to Protobuf");

        // UserAuthor特征写入前采样日志
        userAuthorDataStream = userAuthorDataStream
            .process(new ProcessFunction<Tuple2<String, Map<String, byte[]>>, Tuple2<String, Map<String, byte[]>>>() {
                private static final long SAMPLE_INTERVAL = 60_000L;
                private static final int SAMPLE_COUNT = 3;
                private transient long lastSampleTime;
                private transient int sampleCount;

                @Override
                public void open(Configuration parameters) {
                    lastSampleTime = 0;
                    sampleCount = 0;
                }

                @Override
                public void processElement(Tuple2<String, Map<String, byte[]>> value, Context ctx, Collector<Tuple2<String, Map<String, byte[]>>> out) throws Exception {
                    long now = System.currentTimeMillis();
                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
                        sampleCount = 0;
                    }
                    if (sampleCount < SAMPLE_COUNT) {
                        sampleCount++;
                        int authorCnt = value.f1 != null ? value.f1.size() : 0;
                        LOG.info("[Sample {}/{}] Redis UserAuthorFeature key={} authorCount={}", sampleCount, SAMPLE_COUNT, value.f0, authorCnt);
                    }
                    out.collect(value);
                }
            })
            .name("Sample UserAuthor Feature Output");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(1200); // 20分钟TTL
        
        // User特征sink
        RedisUtil.addRedisSink(userDataStream, redisConfig, true, 100);
        
        // Post特征sink
        RedisUtil.addRedisSink(postDataStream, redisConfig, true, 100);
        
        // UserAuthor特征sink（HashMap格式）
        redisConfig.setCommand("DEL_HMSET");
        RedisUtil.addRedisHashMapSink(userAuthorDataStream, redisConfig, true, 100);

        LOG.info("Job configured, starting execution...");
        String JOB_NAME = "Post Rec Feature 24h Job";
        FlinkMonitorUtil.executeWithMonitor(env, JOB_NAME);
    }

    // ==================== 事件解析 ====================
    
    /**
     * Post事件基础类
     */
    public static class PostEvent {
        public String eventType;
        public long uid;
        public long postId;
        public long authorId;
        public String recToken;
        public int exposedPos;
        public long eventTime;
        
        // 视频/图片相关字段
        public double postLength;
        public double standingTime;
        public double progressTime;
        public double playbackTime;
        
        // 行为标志
        public boolean isLike;
        public boolean isComment;
        public boolean isShare;
        public boolean isFollow;
        public boolean isProfile;
        public boolean isPay;
        public boolean isFavor;
        public boolean isReport;
        public boolean isNotInterest;
        public boolean isCompletePlay;
    }

    /**
     * 事件解析器：解析13种post相关事件
     */
    public static class PostEventParser implements FlatMapFunction<String, PostEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String value, Collector<PostEvent> out) throws Exception {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                
                // 获取 user_event_log 对象
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }
                
                // 获取事件类型（在 user_event_log.event 中）
                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();
                
                // 只处理post相关事件
                if (!isPostEvent(event)) {
                    return;
                }
                
                // 获取 event_data 字段
                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equals(eventData)) {
                    return;
                }
                
                // 解析 event_data 的JSON内容
                JsonNode data = OBJECT_MAPPER.readTree(eventData);
                
                // 提取时间戳
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                }
                
                // 根据不同事件类型解析
                parseEvent(event, data, eventTimeMillis, out);
                
            } catch (Exception e) {
                // 静默处理异常
            }
        }
        
        private boolean isPostEvent(String event) {
            return EVENT_POST_EXPOSURE.equals(event) ||
                   EVENT_VIDEO_PLAY_DURATION.equals(event) ||
                   EVENT_IMAGE_PLAY.equals(event) ||
                   EVENT_VIDEO_PLAY_COMPLETE.equals(event) ||
                   EVENT_POST_LIKE.equals(event) ||
                   EVENT_POST_COMMENT.equals(event) ||
                   EVENT_POST_SHARE.equals(event) ||
                   EVENT_POST_FOLLOW.equals(event) ||
                   EVENT_COLLECT.equals(event) ||
                   EVENT_POST_CLICK_NICKNAME.equals(event) ||
                   EVENT_POST_SWIPE_RIGHT.equals(event) ||
                   EVENT_POST_CLICK_AVATAR.equals(event) ||
                   EVENT_POST_REPORT.equals(event) ||
                   EVENT_POST_UNINTERESTED.equals(event) ||
                   EVENT_POST_PAID.equals(event);
        }
        
        private void parseEvent(String eventType, JsonNode data, long eventTime, Collector<PostEvent> out) {
            if (EVENT_POST_EXPOSURE.equals(eventType)) {
                parsePostExposure(data, eventTime, out);
            } else if (EVENT_VIDEO_PLAY_DURATION.equals(eventType)) {
                parseVideoPlayDuration(data, eventTime, out);
            } else if (EVENT_IMAGE_PLAY.equals(eventType)) {
                parseImagePlay(data, eventTime, out);
            } else if (EVENT_VIDEO_PLAY_COMPLETE.equals(eventType)) {
                parseVideoPlayComplete(data, eventTime, out);
            } else if (EVENT_POST_LIKE.equals(eventType)) {
                parsePostLike(data, eventTime, out);
            } else if (EVENT_POST_COMMENT.equals(eventType)) {
                parsePostComment(data, eventTime, out);
            } else if (EVENT_POST_SHARE.equals(eventType)) {
                parsePostShare(data, eventTime, out);
            } else if (EVENT_POST_FOLLOW.equals(eventType)) {
                parsePostFollow(data, eventTime, out);
            } else if (EVENT_COLLECT.equals(eventType)) {
                parseCollect(data, eventTime, out);
            } else if (EVENT_POST_CLICK_NICKNAME.equals(eventType) ||
                       EVENT_POST_SWIPE_RIGHT.equals(eventType) ||
                       EVENT_POST_CLICK_AVATAR.equals(eventType)) {
                parsePostProfile(data, eventTime, out);
            } else if (EVENT_POST_REPORT.equals(eventType)) {
                parsePostReport(data, eventTime, out);
            } else if (EVENT_POST_UNINTERESTED.equals(eventType)) {
                parsePostUninterested(data, eventTime, out);
            } else if (EVENT_POST_PAID.equals(eventType)) {
                parsePostPaid(data, eventTime, out);
            }
        }
        
        private void parsePostExposure(JsonNode data, long eventTime, Collector<PostEvent> out) {
            JsonNode posts = data.path("posts");
            if (!posts.isArray()) {
                return;
            }
            
            long userId = data.path("userId").asLong(0);
            if (userId <= 0) {
                return;
            }
            
            for (JsonNode post : posts) {
                long postId = parsePostId(post.path("post_id").asText(""));
                int itemType = post.path("item_type").asInt(0);
                int postType = post.path("post_type").asInt(0);
                int exposedPos = post.path("exposed_pos").asInt(0);
                
                // 过滤条件：item_type=1 and post_type in(1,2) and exposed_pos=2
                if (itemType != 1 || (postType != 1 && postType != 2) || exposedPos != 2) {
                    continue;
                }
                
                String recToken = post.path("rec_token").asText("");
                if (recToken == null || recToken.isEmpty()) {
                    continue;
                }
                
                PostEvent evt = new PostEvent();
                evt.eventType = EVENT_POST_EXPOSURE;
                evt.uid = userId;
                evt.postId = postId;
                evt.authorId = 0; // 曝光事件没有author_id
                evt.recToken = recToken;
                evt.exposedPos = exposedPos;
                evt.eventTime = eventTime;
                evt.postLength = post.path("length").asDouble(0);
                
                // 多次上报取首次（这里先都收集，SessionWindow会去重）
                out.collect(evt);
            }
        }
        
        private void parseVideoPlayDuration(JsonNode data, long eventTime, Collector<PostEvent> out) {
            JsonNode list = data.path("list");
            if (!list.isArray()) {
                return;
            }
            
            long userId = data.path("userId").asLong(0);
            if (userId <= 0) {
                return;
            }
            
            for (JsonNode item : list) {
                long postId = parsePostId(item.path("post_id").asText(""));
                int itemType = item.path("item_type").asInt(0);
                int postType = item.path("post_type").asInt(0);
                int exposedPos = item.path("exposed_pos").asInt(0);
                
                if (itemType != 1 || (postType != 1 && postType != 2) || exposedPos != 2) {
                    continue;
                }
                
                String recToken = item.path("rec_token").asText("");
                if (recToken == null || recToken.isEmpty()) {
                    continue;
                }
                
                PostEvent evt = new PostEvent();
                evt.eventType = EVENT_VIDEO_PLAY_DURATION;
                evt.uid = userId;
                evt.postId = postId;
                evt.authorId = item.path("author").asLong(0);
                evt.recToken = recToken;
                evt.exposedPos = exposedPos;
                evt.eventTime = eventTime;
                evt.postLength = item.path("length").asDouble(0);
                evt.standingTime = item.path("standing_time").asDouble(0);
                evt.progressTime = item.path("progress_time").asDouble(0);
                evt.playbackTime = item.path("playback_time").asDouble(0);
                
                out.collect(evt);
            }
        }
        
        private void parseImagePlay(JsonNode data, long eventTime, Collector<PostEvent> out) {
            JsonNode list = data.path("list");
            if (!list.isArray()) {
                return;
            }
            
            long userId = data.path("userId").asLong(0);
            if (userId <= 0) {
                return;
            }
            
            for (JsonNode item : list) {
                long postId = parsePostId(item.path("post_id").asText(""));
                int itemType = item.path("item_type").asInt(0);
                int postType = item.path("post_type").asInt(0);
                int exposedPos = item.path("exposed_pos").asInt(0);
                
                if (itemType != 1 || (postType != 1 && postType != 2) || exposedPos != 2) {
                    continue;
                }
                
                String recToken = item.path("rec_token").asText("");
                if (recToken == null || recToken.isEmpty()) {
                    continue;
                }
                
                PostEvent evt = new PostEvent();
                evt.eventType = EVENT_IMAGE_PLAY;
                evt.uid = userId;
                evt.postId = postId;
                evt.authorId = item.path("author").asLong(0);
                evt.recToken = recToken;
                evt.exposedPos = exposedPos;
                evt.eventTime = eventTime;
                evt.postLength = item.path("length").asDouble(0);
                evt.standingTime = item.path("standing_time").asDouble(0);
                evt.progressTime = item.path("progress_time").asDouble(0);
                
                out.collect(evt);
            }
        }
        
        private void parseVideoPlayComplete(JsonNode data, long eventTime, Collector<PostEvent> out) {
            long postId = parsePostId(data.path("post_id").asText(""));
            long userId = data.path("userId").asLong(0);
            int itemType = data.path("item_type").asInt(0);
            int postType = data.path("post_type").asInt(0);
            int exposedPos = data.path("exposed_pos").asInt(0);
            
            if (postId <= 0 || userId <= 0 || itemType != 1 || (postType != 1 && postType != 2) || exposedPos != 2) {
                return;
            }
            
            String recToken = data.path("rec_token").asText("");
            if (recToken == null || recToken.isEmpty()) {
                return;
            }
            
            PostEvent evt = new PostEvent();
            evt.eventType = EVENT_VIDEO_PLAY_COMPLETE;
            evt.uid = userId;
            evt.postId = postId;
            evt.authorId = data.path("author_id").asLong(0);
            evt.recToken = recToken;
            evt.exposedPos = exposedPos;
            evt.eventTime = eventTime;
            evt.isCompletePlay = true;
            
            out.collect(evt);
        }
        
        private void parsePostLike(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_LIKE, out, (evt) -> {
                String action = data.path("action").asText("");
                evt.isLike = !"unlike".equals(action);
            });
        }
        
        private void parsePostComment(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_COMMENT, out, (evt) -> {
                evt.isComment = true;
            });
        }
        
        private void parsePostShare(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_SHARE, out, (evt) -> {
                evt.isShare = true;
            });
        }
        
        private void parsePostFollow(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_FOLLOW, out, (evt) -> {
                evt.isFollow = true;
            });
        }
        
        private void parseCollect(JsonNode data, long eventTime, Collector<PostEvent> out) {
            long source = data.path("source").asLong(0);
            long userId = data.path("userId").asLong(0);
            int actionType = data.path("action_type").asInt(0);
            
            if (source <= 0 || userId <= 0 || actionType != 1) { // 只要收藏的
                return;
            }
            
            PostEvent evt = new PostEvent();
            evt.eventType = EVENT_COLLECT;
            evt.uid = userId;
            evt.postId = source;
            evt.authorId = 0;
            evt.recToken = "";
            evt.exposedPos = 0;
            evt.eventTime = eventTime;
            evt.isFavor = true;
            
            out.collect(evt);
        }
        
        private void parsePostProfile(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_CLICK_NICKNAME, out, (evt) -> {
                evt.isProfile = true;
            });
        }
        
        private void parsePostReport(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_REPORT, out, (evt) -> {
                evt.isReport = true;
            });
        }
        
        private void parsePostUninterested(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_UNINTERESTED, out, (evt) -> {
                evt.isNotInterest = true;
            });
        }
        
        private void parsePostPaid(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_PAID, out, (evt) -> {
                evt.isPay = true;
            });
        }
        
        private void parseSimpleAction(JsonNode data, long eventTime, String eventType, 
                                      Collector<PostEvent> out, java.util.function.Consumer<PostEvent> setter) {
            long postId = parsePostId(data.path("post_id").asText(""));
            long userId = data.path("userId").asLong(0);
            int itemType = data.path("item_type").asInt(0);
            int postType = data.path("post_type").asInt(0);
            int exposedPos = data.path("exposed_pos").asInt(0);
            
            if (postId <= 0 || userId <= 0 || itemType != 1 || (postType != 1 && postType != 2) || exposedPos != 2) {
                return;
            }
            
            String recToken = data.path("rec_token").asText("");
            if (recToken == null || recToken.isEmpty()) {
                return;
            }
            
            PostEvent evt = new PostEvent();
            evt.eventType = eventType;
            evt.uid = userId;
            evt.postId = postId;
            evt.authorId = data.path("author_id").asLong(0);
            evt.recToken = recToken;
            evt.exposedPos = exposedPos;
            evt.eventTime = eventTime;
            setter.accept(evt);
            
            out.collect(evt);
        }
        
        private long parsePostId(String postIdStr) {
            try {
                return Long.parseLong(postIdStr);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }

    // ==================== SessionWindow聚合 ====================
    
    /**
     * SessionWindow输出摘要
     */
    public static class SessionSummary {
        public String recToken;
        public long uid;
        public long postId;
        public long authorId;
        public double postLength;
        public double standingTime;      // 求和
        public double progressTime;      // 最大值
        public double playbackTime;      // 求和
        public double progressRate;      // max(progress_time) / post_length
        public boolean isFullPlay;
        public boolean isLike;
        public boolean isComment;
        public boolean isDislike;
        public boolean isShare;
        public boolean isFollow;
        public boolean isProfile;
        public boolean isPay;
        public boolean isFavor;
        public boolean isReport;
        public boolean isNotInterest;
        public long eventTime;          // 会话结束时间
    }
    
    /**
     * SessionWindow聚合器
     */
    public static class SessionAggregator implements AggregateFunction<PostEvent, SessionAccumulator, SessionAccumulator> {
        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(PostEvent event, SessionAccumulator acc) {
            // 初始化必填字段
            if (acc.recToken == null) {
                acc.recToken = event.recToken;
                acc.uid = event.uid;
                acc.postId = event.postId;
            }
            // authorId / postLength 可能在首次事件缺失，后续有值时需要覆盖
            if (acc.authorId == 0 && event.authorId > 0) {
                acc.authorId = event.authorId;
            }
            if (acc.postLength == 0 && event.postLength > 0) {
                acc.postLength = event.postLength;
            }
            
            // 聚合时长数据
            acc.standingTime += event.standingTime;
            acc.progressTime = Math.max(acc.progressTime, event.progressTime);
            acc.playbackTime += event.playbackTime;
            
            // 聚合行为标志（取或操作）
            acc.isFullPlay = acc.isFullPlay || event.isCompletePlay;
            acc.isLike = acc.isLike || event.isLike;
            acc.isComment = acc.isComment || event.isComment;
            acc.isDislike = acc.isDislike || event.isNotInterest;
            acc.isShare = acc.isShare || event.isShare;
            acc.isFollow = acc.isFollow || event.isFollow;
            acc.isProfile = acc.isProfile || event.isProfile;
            acc.isPay = acc.isPay || event.isPay;
            acc.isFavor = acc.isFavor || event.isFavor;
            acc.isReport = acc.isReport || event.isReport;
            acc.isNotInterest = acc.isNotInterest || event.isNotInterest;
            
            return acc;
        }

        @Override
        public SessionAccumulator getResult(SessionAccumulator acc) {
            return acc;
        }

        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            a.standingTime += b.standingTime;
            a.progressTime = Math.max(a.progressTime, b.progressTime);
            a.playbackTime += b.playbackTime;
            a.isFullPlay = a.isFullPlay || b.isFullPlay;
            a.isLike = a.isLike || b.isLike;
            a.isComment = a.isComment || b.isComment;
            a.isDislike = a.isDislike || b.isDislike;
            a.isShare = a.isShare || b.isShare;
            a.isFollow = a.isFollow || b.isFollow;
            a.isProfile = a.isProfile || b.isProfile;
            a.isPay = a.isPay || b.isPay;
            a.isFavor = a.isFavor || b.isFavor;
            a.isReport = a.isReport || b.isReport;
            a.isNotInterest = a.isNotInterest || b.isNotInterest;
            return a;
        }
    }
    
    public static class SessionAccumulator {
        public String recToken;
        public long uid;
        public long postId;
        public long authorId;
        public double postLength;
        public double standingTime;
        public double progressTime;
        public double playbackTime;
        public boolean isFullPlay;
        public boolean isLike;
        public boolean isComment;
        public boolean isDislike;
        public boolean isShare;
        public boolean isFollow;
        public boolean isProfile;
        public boolean isPay;
        public boolean isFavor;
        public boolean isReport;
        public boolean isNotInterest;
    }
    
    /**
     * SessionWindow处理函数：计算progressRate并输出SessionSummary
     */
    public static class SessionWindowFunction extends ProcessWindowFunction<SessionAccumulator, SessionSummary, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<SessionAccumulator> elements, Collector<SessionSummary> out) throws Exception {
            SessionAccumulator acc = elements.iterator().next();
            if (acc == null || acc.recToken == null) {
                return;
            }
            
            SessionSummary summary = new SessionSummary();
            summary.recToken = acc.recToken;
            summary.uid = acc.uid;
            summary.postId = acc.postId;
            summary.authorId = acc.authorId;
            summary.postLength = acc.postLength;
            summary.standingTime = acc.standingTime;
            summary.progressTime = acc.progressTime;
            summary.playbackTime = acc.playbackTime;
            summary.progressRate = acc.postLength > 0 ? acc.progressTime / acc.postLength : 0;
            summary.isFullPlay = acc.isFullPlay;
            summary.isLike = acc.isLike;
            summary.isComment = acc.isComment;
            summary.isDislike = acc.isDislike;
            summary.isShare = acc.isShare;
            summary.isFollow = acc.isFollow;
            summary.isProfile = acc.isProfile;
            summary.isPay = acc.isPay;
            summary.isFavor = acc.isFavor;
            summary.isReport = acc.isReport;
            summary.isNotInterest = acc.isNotInterest;
            summary.eventTime = context.window().getEnd();
            
            out.collect(summary);
        }
    }

    // ==================== User侧特征聚合 ====================
    
    public static class UserFeature24hAggregation {
        public long uid;
        
        // 计数特征
        public int expPostCnt;
        public int skipPostCnt;
        
        // 列表特征（按最近顺序维护，避免频繁排序）
        public LinkedHashMap<Long, PostEntry> completePlayPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> skipPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostScoreEntry> stand10sPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostScoreEntry> play8sPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> likePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> commentPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> sharePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> followPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> collectPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> profilePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> payPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> reportPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> dislikePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> positivePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> negativePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, AuthorEntry> positiveAuthors = new LinkedHashMap<>();
        public LinkedHashMap<Long, AuthorEntry> negativeAuthors = new LinkedHashMap<>();
    }
    
    public static class PostEntry {
        public long postId;
        public long timestamp;
    }
    
    public static class PostScoreEntry {
        public long postId;
        public float score;
        public long timestamp;
    }
    
    public static class AuthorEntry {
        public long authorId;
        public long timestamp;
    }
    
    /**
     * User特征第一阶段聚合器（分片聚合）
     */
    public static class UserFeatureAggregator implements AggregateFunction<SessionSummary, UserFeatureAccumulator, UserFeature24hAggregation> {
        @Override
        public UserFeatureAccumulator createAccumulator() {
            return new UserFeatureAccumulator();
        }

        @Override
        public UserFeatureAccumulator add(SessionSummary summary, UserFeatureAccumulator acc) {
            acc.uid = summary.uid;
            if (acc.totalEventCount >= MAX_EVENTS_PER_USER_WINDOW) {
                return acc;
            }
            
            // 计数特征
            acc.expPostCnt++;
            if (summary.progressTime <= 2) {
                acc.skipPostCnt++;
            }
            
            // 列表特征（保持最近顺序）
            if (summary.isFullPlay) {
                addRecentPost(acc.completePlayPosts, summary.postId, summary.eventTime, 30);
            }
            if (summary.progressTime <= 2) {
                addRecentPost(acc.skipPosts, summary.postId, summary.eventTime, 30);
            }
            if (summary.standingTime > 10) {
                addRecentPostScore(acc.stand10sPosts, summary.postId, (float) summary.standingTime, summary.eventTime, 30);
            }
            if (summary.playbackTime > 8) {
                addRecentPostScore(acc.play8sPosts, summary.postId, (float) summary.playbackTime, summary.eventTime, 30);
            }
            if (summary.isLike) {
                addRecentPost(acc.likePosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isComment) {
                addRecentPost(acc.commentPosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isShare) {
                addRecentPost(acc.sharePosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isFollow) {
                addRecentPost(acc.followPosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isFavor) {
                addRecentPost(acc.collectPosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isProfile) {
                addRecentPost(acc.profilePosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isPay) {
                addRecentPost(acc.payPosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isReport) {
                addRecentPost(acc.reportPosts, summary.postId, summary.eventTime, 10);
            }
            if (summary.isNotInterest) {
                addRecentPost(acc.dislikePosts, summary.postId, summary.eventTime, 10);
            }
            
            // 正反馈：点赞|评论|分享|点关注|收藏|点作者主页|付费
            if (summary.isLike || summary.isComment || summary.isShare || summary.isFollow || 
                summary.isFavor || summary.isProfile || summary.isPay) {
                addRecentPost(acc.positivePosts, summary.postId, summary.eventTime, 20);
                if (summary.authorId > 0) {
                    addRecentAuthor(acc.positiveAuthors, summary.authorId, summary.eventTime, 20);
                }
            }
            
            // 负反馈：举报|不喜欢
            if (summary.isReport || summary.isNotInterest) {
                addRecentPost(acc.negativePosts, summary.postId, summary.eventTime, 20);
                if (summary.authorId > 0) {
                    addRecentAuthor(acc.negativeAuthors, summary.authorId, summary.eventTime, 20);
                }
            }
            acc.totalEventCount++;
            return acc;
        }

        @Override
        public UserFeature24hAggregation getResult(UserFeatureAccumulator acc) {
            UserFeature24hAggregation result = new UserFeature24hAggregation();
            result.uid = acc.uid;
            result.expPostCnt = acc.expPostCnt;
            result.skipPostCnt = acc.skipPostCnt;
            result.completePlayPosts = new LinkedHashMap<>(acc.completePlayPosts);
            result.skipPosts = new LinkedHashMap<>(acc.skipPosts);
            result.stand10sPosts = new LinkedHashMap<>(acc.stand10sPosts);
            result.play8sPosts = new LinkedHashMap<>(acc.play8sPosts);
            result.likePosts = new LinkedHashMap<>(acc.likePosts);
            result.commentPosts = new LinkedHashMap<>(acc.commentPosts);
            result.sharePosts = new LinkedHashMap<>(acc.sharePosts);
            result.followPosts = new LinkedHashMap<>(acc.followPosts);
            result.collectPosts = new LinkedHashMap<>(acc.collectPosts);
            result.profilePosts = new LinkedHashMap<>(acc.profilePosts);
            result.payPosts = new LinkedHashMap<>(acc.payPosts);
            result.reportPosts = new LinkedHashMap<>(acc.reportPosts);
            result.dislikePosts = new LinkedHashMap<>(acc.dislikePosts);
            result.positivePosts = new LinkedHashMap<>(acc.positivePosts);
            result.negativePosts = new LinkedHashMap<>(acc.negativePosts);
            result.positiveAuthors = new LinkedHashMap<>(acc.positiveAuthors);
            result.negativeAuthors = new LinkedHashMap<>(acc.negativeAuthors);
            return result;
        }

        @Override
        public UserFeatureAccumulator merge(UserFeatureAccumulator a, UserFeatureAccumulator b) {
            a.expPostCnt += b.expPostCnt;
            a.skipPostCnt += b.skipPostCnt;
            a.totalEventCount += b.totalEventCount;
            mergeRecentPost(a.completePlayPosts, b.completePlayPosts, 30);
            mergeRecentPost(a.skipPosts, b.skipPosts, 30);
            mergeRecentPostScore(a.stand10sPosts, b.stand10sPosts, 30);
            mergeRecentPostScore(a.play8sPosts, b.play8sPosts, 30);
            mergeRecentPost(a.likePosts, b.likePosts, 10);
            mergeRecentPost(a.commentPosts, b.commentPosts, 10);
            mergeRecentPost(a.sharePosts, b.sharePosts, 10);
            mergeRecentPost(a.followPosts, b.followPosts, 10);
            mergeRecentPost(a.collectPosts, b.collectPosts, 10);
            mergeRecentPost(a.profilePosts, b.profilePosts, 10);
            mergeRecentPost(a.payPosts, b.payPosts, 10);
            mergeRecentPost(a.reportPosts, b.reportPosts, 10);
            mergeRecentPost(a.dislikePosts, b.dislikePosts, 10);
            mergeRecentPost(a.positivePosts, b.positivePosts, 20);
            mergeRecentPost(a.negativePosts, b.negativePosts, 20);
            mergeRecentAuthor(a.positiveAuthors, b.positiveAuthors, 20);
            mergeRecentAuthor(a.negativeAuthors, b.negativeAuthors, 20);
            return a;
        }
    }
    
    /**
     * User特征第二阶段聚合器（全局聚合）
     */
    public static class UserFeatureAccumulator {
        public long uid;
        public int expPostCnt;
        public int skipPostCnt;
        public int totalEventCount = 0;
        public LinkedHashMap<Long, PostEntry> completePlayPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> skipPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostScoreEntry> stand10sPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostScoreEntry> play8sPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> likePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> commentPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> sharePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> followPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> collectPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> profilePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> payPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> reportPosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> dislikePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> positivePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, PostEntry> negativePosts = new LinkedHashMap<>();
        public LinkedHashMap<Long, AuthorEntry> positiveAuthors = new LinkedHashMap<>();
        public LinkedHashMap<Long, AuthorEntry> negativeAuthors = new LinkedHashMap<>();
    }
    
    // ==================== Post侧特征聚合 ====================
    
    public static class PostFeature24hAggregation {
        public long postId;
        public int expCnt;
        public int skipCnt;
        public int completeCnt;
        public double totalProgressRate;
        public int progressRateCount;
        public int stand10sCnt;
        public int stand20sCnt;
        public int play5sCnt;
        public int play10sCnt;
        public int likeCnt;
        public int commentCnt;
        public int shareCnt;
        public int collectCnt;
        public int profileCnt;
        public int reportCnt;
        public int dislikeCnt;
        public int payCnt;
    }
    
    public static class PostFeatureAggregator implements AggregateFunction<SessionSummary, PostFeatureAccumulator, PostFeature24hAggregation> {
        @Override
        public PostFeatureAccumulator createAccumulator() {
            return new PostFeatureAccumulator();
        }

        @Override
        public PostFeatureAccumulator add(SessionSummary summary, PostFeatureAccumulator acc) {
            acc.postId = summary.postId;
            acc.expCnt++;
            
            if (summary.progressTime <= 2) {
                acc.skipCnt++;
            }
            if (summary.isFullPlay) {
                acc.completeCnt++;
            }
            if (summary.progressRate > 0) {
                acc.totalProgressRate += summary.progressRate;
                acc.progressRateCount++;
            }
            if (summary.standingTime > 10) {
                acc.stand10sCnt++;
            }
            if (summary.standingTime > 20) {
                acc.stand20sCnt++;
            }
            if (summary.playbackTime > 5) {
                acc.play5sCnt++;
            }
            if (summary.playbackTime > 10) {
                acc.play10sCnt++;
            }
            if (summary.isLike) {
                acc.likeCnt++;
            }
            if (summary.isComment) {
                acc.commentCnt++;
            }
            if (summary.isShare) {
                acc.shareCnt++;
            }
            if (summary.isFavor) {
                acc.collectCnt++;
            }
            if (summary.isProfile) {
                acc.profileCnt++;
            }
            if (summary.isReport) {
                acc.reportCnt++;
            }
            if (summary.isNotInterest) {
                acc.dislikeCnt++;
            }
            if (summary.isPay) {
                acc.payCnt++;
            }
            
            return acc;
        }

        @Override
        public PostFeature24hAggregation getResult(PostFeatureAccumulator acc) {
            PostFeature24hAggregation result = new PostFeature24hAggregation();
            result.postId = acc.postId;
            result.expCnt = acc.expCnt;
            result.skipCnt = acc.skipCnt;
            result.completeCnt = acc.completeCnt;
            result.totalProgressRate = acc.totalProgressRate;
            result.progressRateCount = acc.progressRateCount;
            result.stand10sCnt = acc.stand10sCnt;
            result.stand20sCnt = acc.stand20sCnt;
            result.play5sCnt = acc.play5sCnt;
            result.play10sCnt = acc.play10sCnt;
            result.likeCnt = acc.likeCnt;
            result.commentCnt = acc.commentCnt;
            result.shareCnt = acc.shareCnt;
            result.collectCnt = acc.collectCnt;
            result.profileCnt = acc.profileCnt;
            result.reportCnt = acc.reportCnt;
            result.dislikeCnt = acc.dislikeCnt;
            result.payCnt = acc.payCnt;
            return result;
        }

        @Override
        public PostFeatureAccumulator merge(PostFeatureAccumulator a, PostFeatureAccumulator b) {
            a.expCnt += b.expCnt;
            a.skipCnt += b.skipCnt;
            a.completeCnt += b.completeCnt;
            a.totalProgressRate += b.totalProgressRate;
            a.progressRateCount += b.progressRateCount;
            a.stand10sCnt += b.stand10sCnt;
            a.stand20sCnt += b.stand20sCnt;
            a.play5sCnt += b.play5sCnt;
            a.play10sCnt += b.play10sCnt;
            a.likeCnt += b.likeCnt;
            a.commentCnt += b.commentCnt;
            a.shareCnt += b.shareCnt;
            a.collectCnt += b.collectCnt;
            a.profileCnt += b.profileCnt;
            a.reportCnt += b.reportCnt;
            a.dislikeCnt += b.dislikeCnt;
            a.payCnt += b.payCnt;
            return a;
        }
    }
    
    public static class PostFeatureAccumulator {
        public long postId;
        public int expCnt;
        public int skipCnt;
        public int completeCnt;
        public double totalProgressRate;
        public int progressRateCount;
        public int stand10sCnt;
        public int stand20sCnt;
        public int play5sCnt;
        public int play10sCnt;
        public int likeCnt;
        public int commentCnt;
        public int shareCnt;
        public int collectCnt;
        public int profileCnt;
        public int reportCnt;
        public int dislikeCnt;
        public int payCnt;
    }

    // ==================== UserAuthor侧特征聚合（按uid汇总作者，单阶段限流） ====================
    
    public static class UserAuthorFeatureMapAggregation {
        public long uid;
        public Map<String, byte[]> authorFeatures;
    }
    
    public static class AuthorCounts {
        public int expCnt;
        public int skipCnt;
        public int play8sCnt;
        public int play12sCnt;
        public int stand10sCnt;
        public int stand20sCnt;
        public int likeCnt;
        public int commentCnt;
        public int shareCnt;
        public int followCnt;
        public int collectCnt;
        public int profileCnt;
        public int reportCnt;
        public int dislikeCnt;
        public int payCnt;
    }
    
    public static class UserAuthorMapAccumulator {
        public long uid;
        public int totalEventCount = 0;
        public Map<Long, AuthorCounts> authorMap = new HashMap<>();
    }
    
    /**
     * UserAuthor特征聚合器：按uid汇总作者，限流
     */
    public static class UserAuthorMapAggregator implements AggregateFunction<SessionSummary, UserAuthorMapAccumulator, UserAuthorFeatureMapAggregation> {
        @Override
        public UserAuthorMapAccumulator createAccumulator() {
            return new UserAuthorMapAccumulator();
        }

        @Override
        public UserAuthorMapAccumulator add(SessionSummary summary, UserAuthorMapAccumulator acc) {
            acc.uid = summary.uid;
            if (acc.totalEventCount >= MAX_EVENTS_PER_USER_WINDOW) {
                return acc;
            }
            AuthorCounts counts = acc.authorMap.computeIfAbsent(summary.authorId, k -> new AuthorCounts());
            counts.expCnt++;
            if (summary.progressTime <= 2) counts.skipCnt++;
            if (summary.playbackTime > 8) counts.play8sCnt++;
            if (summary.playbackTime > 12) counts.play12sCnt++;
            if (summary.standingTime > 10) counts.stand10sCnt++;
            if (summary.standingTime > 20) counts.stand20sCnt++;
            if (summary.isLike) counts.likeCnt++;
            if (summary.isComment) counts.commentCnt++;
            if (summary.isShare) counts.shareCnt++;
            if (summary.isFollow) counts.followCnt++;
            if (summary.isFavor) counts.collectCnt++;
            if (summary.isProfile) counts.profileCnt++;
            if (summary.isReport) counts.reportCnt++;
            if (summary.isNotInterest) counts.dislikeCnt++;
            if (summary.isPay) counts.payCnt++;
            acc.totalEventCount++;
            return acc;
        }

        @Override
        public UserAuthorFeatureMapAggregation getResult(UserAuthorMapAccumulator acc) {
            if (acc.authorMap.isEmpty()) {
                return null;
            }
            UserAuthorFeatureMapAggregation result = new UserAuthorFeatureMapAggregation();
            result.uid = acc.uid;
            result.authorFeatures = new HashMap<>();
            for (Map.Entry<Long, AuthorCounts> e : acc.authorMap.entrySet()) {
                result.authorFeatures.put(String.valueOf(e.getKey()),
                    buildUserAuthorFeatureProto(acc.uid, e.getKey(), e.getValue()));
            }
            return result;
        }

        @Override
        public UserAuthorMapAccumulator merge(UserAuthorMapAccumulator a, UserAuthorMapAccumulator b) {
            a.totalEventCount += b.totalEventCount;
            for (Map.Entry<Long, AuthorCounts> entry : b.authorMap.entrySet()) {
                AuthorCounts target = a.authorMap.computeIfAbsent(entry.getKey(), k -> new AuthorCounts());
                AuthorCounts src = entry.getValue();
                target.expCnt += src.expCnt;
                target.skipCnt += src.skipCnt;
                target.play8sCnt += src.play8sCnt;
                target.play12sCnt += src.play12sCnt;
                target.stand10sCnt += src.stand10sCnt;
                target.stand20sCnt += src.stand20sCnt;
                target.likeCnt += src.likeCnt;
                target.commentCnt += src.commentCnt;
                target.shareCnt += src.shareCnt;
                target.followCnt += src.followCnt;
                target.collectCnt += src.collectCnt;
                target.profileCnt += src.profileCnt;
                target.reportCnt += src.reportCnt;
                target.dislikeCnt += src.dislikeCnt;
                target.payCnt += src.payCnt;
            }
            return a;
        }
    }

    // ==================== Protobuf构建 ====================
    
    private static byte[] buildUserFeatureProto(UserFeature24hAggregation agg) {
        RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();
        
        builder.setViewerExpPostCnt24H(agg.expPostCnt);
        builder.setViewerSkipPostCnt24H(agg.skipPostCnt);
        
        // 完播列表（截断最近的30个）
        for (PostEntry p : agg.completePlayPosts.values()) {
            builder.addViewerCompleteplayPostList24H(p.postId);
        }
        
        // 快划列表（截断最近的30个）
        for (PostEntry p : agg.skipPosts.values()) {
            builder.addViewerSkipPostList24H(p.postId);
        }
        
        // 停留10s以上列表（截断最近的30个）
        for (PostScoreEntry p : agg.stand10sPosts.values()) {
            builder.addViewer10SstandPostList24H(
                RecFeature.IdScore.newBuilder()
                    .setId(p.postId)
                    .setScore((float)p.score)
                    .build()
            );
        }
        
        // 观看8s以上列表（截断最近的30个）
        for (PostScoreEntry p : agg.play8sPosts.values()) {
            builder.addViewer8SplayPostList24H(
                RecFeature.IdScore.newBuilder()
                    .setId(p.postId)
                    .setScore((float)p.score)
                    .build()
            );
        }
        
        // 点赞列表（截断最近的10个）
        for (PostEntry p : agg.likePosts.values()) {
            builder.addViewerLikePostList24H(p.postId);
        }
        
        // 评论列表（截断最近的10个）
        for (PostEntry p : agg.commentPosts.values()) {
            builder.addViewerCommentPostList24H(p.postId);
        }
        
        // 分享列表（截断最近的10个）
        for (PostEntry p : agg.sharePosts.values()) {
            builder.addViewerSharePostList24H(p.postId);
        }
        
        // 点关注列表（截断最近的10个）- 使用his字段
        for (PostEntry p : agg.followPosts.values()) {
            builder.addViewerFollowPostHis24H(p.postId);
        }
        
        // 收藏列表（截断最近的10个）
        for (PostEntry p : agg.collectPosts.values()) {
            builder.addViewerCollectPostList24H(p.postId);
        }
        
        // 点作者主页列表（截断最近的10个）- 使用his字段
        for (PostEntry p : agg.profilePosts.values()) {
            builder.addViewerProfilePostHis24H(p.postId);
        }
        
        // 付费列表（截断最近的10个）
        for (PostEntry p : agg.payPosts.values()) {
            builder.addViewerPayPostList24H(p.postId);
        }
        
        // 举报列表（截断最近的10个）
        for (PostEntry p : agg.reportPosts.values()) {
            builder.addViewerReportPostList24H(p.postId);
        }
        
        // 不喜欢列表（截断最近的10个）
        for (PostEntry p : agg.dislikePosts.values()) {
            builder.addViewerDislikePostList24H(p.postId);
        }
        
        // 正反馈列表（截断最近的20个）
        for (PostEntry p : agg.positivePosts.values()) {
            builder.addViewerPositivePostList24H(p.postId);
        }
        
        // 负反馈列表（截断最近的20个）
        for (PostEntry p : agg.negativePosts.values()) {
            builder.addViewerNegetivePostList24H(p.postId);
        }
        
        // 正反馈作者列表（截断最近的20个）
        for (AuthorEntry a : agg.positiveAuthors.values()) {
            builder.addViewerPositiveAuthorList24H(a.authorId);
        }
        
        // 负反馈作者列表（截断最近的20个）
        for (AuthorEntry a : agg.negativeAuthors.values()) {
            builder.addViewerNegetiveAuthorList24H(a.authorId);
        }
        
        return builder.build().toByteArray();
    }
    
    private static byte[] buildPostFeatureProto(PostFeature24hAggregation agg) {
        RecFeature.RecPostFeature.Builder builder = RecFeature.RecPostFeature.newBuilder();
        
        builder.setPostExpCnt24H(agg.expCnt);
        builder.setPostSkipCnt24H(agg.skipCnt);
        builder.setPostCompeleteCnt24H(agg.completeCnt);
        builder.setPostProgressRate24H(agg.progressRateCount > 0 ? 
            (float)(agg.totalProgressRate / agg.progressRateCount) : 0.0f);
        builder.setPost10SstandCnt24H(agg.stand10sCnt);
        builder.setPost20SstandCnt24H(agg.stand20sCnt);
        builder.setPost5SplayCnt24H(agg.play5sCnt);
        builder.setPost10SplayCnt24H(agg.play10sCnt);
        builder.setPostLikeCnt24H(agg.likeCnt);
        builder.setPostCommentCnt24H(agg.commentCnt);
        builder.setPostShareCnt24H(agg.shareCnt);
        builder.setPostCollectCnt24H(agg.collectCnt);
        builder.setPostProfileCnt24H(agg.profileCnt);
        builder.setPostReportCnt24H(agg.reportCnt);
        builder.setPostDislikeCnt24H(agg.dislikeCnt);
        builder.setPostPayCnt24H(agg.payCnt);
        
        return builder.build().toByteArray();
    }
    
    private static byte[] buildUserAuthorFeatureProto(long uid, long authorId, AuthorCounts c) {
        RecFeature.RecUserAuthorFeature.Builder builder = RecFeature.RecUserAuthorFeature.newBuilder();
        
        builder.setUserId(uid);
        builder.setAuthorId(authorId);
        builder.setUserauthorExpCnt24H(c.expCnt);
        builder.setUserauthorSkipCnt24H(c.skipCnt);
        builder.setUserauthor8SviewCnt24H(c.play8sCnt);
        builder.setUserauthor12SplayCnt24H(c.play12sCnt);
        builder.setUserauthor10SstandCnt24H(c.stand10sCnt);
        builder.setUserauthor20SstandCnt24H(c.stand20sCnt);
        builder.setUserauthorLikeCnt24H(c.likeCnt);
        builder.setUserauthorCommentCnt24H(c.commentCnt);
        builder.setUserauthorShareCnt24H(c.shareCnt);
        builder.setUserauthorFollowCnt24H(c.followCnt);
        builder.setUserauthorCollectCnt24H(c.collectCnt);
        builder.setUserauthorProfileCnt24H(c.profileCnt);
        builder.setUserauthorReportCnt24H(c.reportCnt);
        builder.setUserauthorDislikeCnt24H(c.dislikeCnt);
        builder.setUserauthorPayCnt24H(c.payCnt);
        
        return builder.build().toByteArray();
    }

    // ==================== 工具方法 ====================
    
    private static void addRecentPost(LinkedHashMap<Long, PostEntry> map, long postId, long timestamp, int maxSize) {
        PostEntry existing = map.get(postId);
        if (existing != null) {
            if (timestamp > existing.timestamp) {
                map.remove(postId);
                PostEntry entry = new PostEntry();
                entry.postId = postId;
                entry.timestamp = timestamp;
                map.put(postId, entry);
            }
        } else {
            PostEntry entry = new PostEntry();
            entry.postId = postId;
            entry.timestamp = timestamp;
            map.put(postId, entry);
            if (map.size() > maxSize) {
                Iterator<Long> it = map.keySet().iterator();
                if (it.hasNext()) {
                    it.next();
                    it.remove();
                }
            }
        }
    }
    
    private static void addRecentPostScore(LinkedHashMap<Long, PostScoreEntry> map, long postId, float score, long timestamp, int maxSize) {
        PostScoreEntry existing = map.get(postId);
        if (existing != null) {
            if (timestamp > existing.timestamp) {
                map.remove(postId);
                PostScoreEntry entry = new PostScoreEntry();
                entry.postId = postId;
                entry.score = score;
                entry.timestamp = timestamp;
                map.put(postId, entry);
            }
        } else {
            PostScoreEntry entry = new PostScoreEntry();
            entry.postId = postId;
            entry.score = score;
            entry.timestamp = timestamp;
            map.put(postId, entry);
            if (map.size() > maxSize) {
                Iterator<Long> it = map.keySet().iterator();
                if (it.hasNext()) {
                    it.next();
                    it.remove();
                }
            }
        }
    }
    
    private static void addRecentAuthor(LinkedHashMap<Long, AuthorEntry> map, long authorId, long timestamp, int maxSize) {
        AuthorEntry existing = map.get(authorId);
        if (existing != null) {
            if (timestamp > existing.timestamp) {
                map.remove(authorId);
                AuthorEntry entry = new AuthorEntry();
                entry.authorId = authorId;
                entry.timestamp = timestamp;
                map.put(authorId, entry);
            }
        } else {
            AuthorEntry entry = new AuthorEntry();
            entry.authorId = authorId;
            entry.timestamp = timestamp;
            map.put(authorId, entry);
            if (map.size() > maxSize) {
                Iterator<Long> it = map.keySet().iterator();
                if (it.hasNext()) {
                    it.next();
                    it.remove();
                }
            }
        }
    }
    
    private static void mergeRecentPost(LinkedHashMap<Long, PostEntry> target, LinkedHashMap<Long, PostEntry> source, int maxSize) {
        for (PostEntry entry : source.values()) {
            addRecentPost(target, entry.postId, entry.timestamp, maxSize);
        }
    }
    
    private static void mergeRecentPostScore(LinkedHashMap<Long, PostScoreEntry> target, LinkedHashMap<Long, PostScoreEntry> source, int maxSize) {
        for (PostScoreEntry entry : source.values()) {
            addRecentPostScore(target, entry.postId, entry.score, entry.timestamp, maxSize);
        }
    }
    
    private static void mergeRecentAuthor(LinkedHashMap<Long, AuthorEntry> target, LinkedHashMap<Long, AuthorEntry> source, int maxSize) {
        for (AuthorEntry entry : source.values()) {
            addRecentAuthor(target, entry.authorId, entry.timestamp, maxSize);
        }
    }
}
