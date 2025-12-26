package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
 
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
 
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class RecUserPostFeatureLatestJob {
    private static final Logger LOG = LoggerFactory.getLogger(RecUserPostFeatureLatestJob.class);
    
    // Redis key前缀
    private static final String USER_PREFIX = "rec:user_feature:{";
    private static final String USER_SUFFIX = "}:postlatest";
    
    
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
    
    // SessionWindow gap: 70秒
    private static final long SESSION_GAP_SECONDES = 70;
    
    // 单用户/用户作者窗口内事件上限
    private static final int MAX_EVENTS_PER_USER_WINDOW = 300;

    public static void main(String[] args) throws Exception {
        System.out.println("=== RecUserPostFeatureLatestJob Starting ===");
        System.out.println("LOG instance: " + LOG.getClass().getName());
        LOG.info("========================================");
        LOG.info("Starting RecUserPostFeatureLatestJob");
        LOG.info("Processing post events from advertise topic");
        LOG.info("SessionWindow gap: {} seconds", SESSION_GAP_SECONDES);
        LOG.info("========================================");
        System.out.println("=== RecUserPostFeatureLatestJob Logs Printed ===");
        
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
                    .withIdleness(Duration.ofMinutes(1)) // 添加空闲检测，避免水位线停滞
            );

        // 3.2 SessionWindow聚合：基于rec_token + post_id
        SingleOutputStreamOperator<SessionSummary> sessionStream = eventStream
            .keyBy(new KeySelector<PostEvent, String>() {
                @Override
                public String getKey(PostEvent value) throws Exception {
                    return value.recToken + "|" + value.postId;
                }
            })
            .window(EventTimeSessionWindows.withGap(org.apache.flink.streaming.api.windowing.time.Time.seconds(SESSION_GAP_SECONDES)))
            .trigger(new SessionFeatureTrigger())
            .aggregate(new SessionAggregator(), new SessionWindowFunction())
            .name("Session Window Aggregation");

        // 4.1 User侧特征：使用自定义 process 窗口（按 uid key），保存每个特征队列并及时写入 Redis
        SingleOutputStreamOperator<Tuple2<String, byte[]>> userFeatureProtoStream = sessionStream
            .keyBy((KeySelector<SessionSummary, Long>) value -> value.uid)
            .process(new UserRecentFeatureProcess())
            .name("User Recent Feature Process");

        // 第五步：转换为Protobuf并写入Redis
        // userFeatureProtoStream 已经输出 Tuple2<redisKey, bytes>

        // 第六步：创建sink，Redis环境（仅写入用户最近行为）
        Properties redisProps = RedisUtil.loadProperties();
        
        // User特征sink（使用SET命令，key-value结构），TTL 24小时
        RedisConfig userRedisConfig = RedisConfig.fromProperties(redisProps);
        userRedisConfig.setTtl(24 * 3600); // 24小时TTL
        userRedisConfig.setCommand("SET"); // 明确设置为SET命令
        RedisUtil.addRedisSink(userFeatureProtoStream, userRedisConfig, true, 10);

        LOG.info("Job configured, starting execution...");
        String JOB_NAME = "Rec User Post Feature Latest Job";
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

    /**
     * Custom Trigger that fires (without purging) when an incoming PostEvent satisfies session feature conditions.
     * It also tracks a per-window fired flag to avoid repeated fires for the same session window.
     */
    public static class SessionFeatureTrigger extends org.apache.flink.streaming.api.windowing.triggers.Trigger<PostEvent, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private final org.apache.flink.api.common.state.ValueStateDescriptor<Boolean> firedDesc =
            new org.apache.flink.api.common.state.ValueStateDescriptor<>("sessionFired", Types.BOOLEAN);

        @Override
        public org.apache.flink.streaming.api.windowing.triggers.TriggerResult onElement(PostEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            org.apache.flink.api.common.state.ValueState<Boolean> firedState = ctx.getPartitionedState(firedDesc);
            Boolean alreadyFired = firedState.value();
            if (Boolean.TRUE.equals(alreadyFired)) {
                return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.CONTINUE;
            }

            // Trigger conditions (same semantics as downstream): full play / standing >10s / playback >8s / explicit feedback flags
            boolean shouldFire = false;
            if (element != null) {
                if (element.isCompletePlay) shouldFire = true;
                if (element.standingTime > 10) shouldFire = true;
                if (element.playbackTime > 8) shouldFire = true;
                if (element.isLike || element.isComment || element.isShare || element.isFollow ||
                    element.isFavor || element.isProfile || element.isPay || element.isReport || element.isNotInterest) {
                    shouldFire = true;
                }
            }

            if (shouldFire) {
                firedState.update(true);
                return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.FIRE;
            }
            return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.CONTINUE;
        }

        @Override
        public org.apache.flink.streaming.api.windowing.triggers.TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // Fire when window's max timestamp is reached (normal session close)
            if (time >= window.maxTimestamp()) {
                return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.FIRE;
            }
            return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.CONTINUE;
        }

        @Override
        public org.apache.flink.streaming.api.windowing.triggers.TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return org.apache.flink.streaming.api.windowing.triggers.TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            org.apache.flink.api.common.state.ValueState<Boolean> firedState = ctx.getPartitionedState(firedDesc);
            firedState.clear();
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, org.apache.flink.streaming.api.windowing.triggers.Trigger.OnMergeContext ctx) throws Exception {
            // Merge partitioned fired state using framework merge helper.
            ctx.mergePartitionedState((org.apache.flink.api.common.state.StateDescriptor) firedDesc);
        }
    }

    // ==================== User侧特征聚合 ====================
    
    public static class UserFeature24hAggregation {
        public long uid;
        
        // 计数特征
        public int expPostCnt;
        
        
        // 列表特征（按最近顺序维护，避免频繁排序）
        public LinkedHashMap<Long, PostEntry> completePlayPosts = new LinkedHashMap<>();
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
            
            
            // 列表特征（保持最近顺序）
            if (summary.isFullPlay) {
                addRecentPost(acc.completePlayPosts, summary.postId, summary.eventTime, 30);
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
            result.completePlayPosts = new LinkedHashMap<>(acc.completePlayPosts);
            
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
            
            a.totalEventCount += b.totalEventCount;
            mergeRecentPost(a.completePlayPosts, b.completePlayPosts, 30);
            
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
        public int totalEventCount = 0;
        public LinkedHashMap<Long, PostEntry> completePlayPosts = new LinkedHashMap<>();
        
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
    
    

    

    // ==================== Protobuf构建 ====================
    
    /**
     * KeyedProcessFunction 按 uid 维护每个用户的近期行为队列（保留 12 小时），并在每次更新后输出 Redis 写入条目
     */
    public static class UserRecentFeatureProcess extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Long, SessionSummary, Tuple2<String, byte[]>> {
        private transient org.apache.flink.api.common.state.ListState<PostEntry> completePlayState;
        private transient org.apache.flink.api.common.state.ListState<PostScoreEntry> stand10State;
        private transient org.apache.flink.api.common.state.ListState<PostScoreEntry> play8State;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> likeState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> commentState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> shareState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> followState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> collectState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> profileState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> payState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> reportState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> dislikeState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> positiveState;
        private transient org.apache.flink.api.common.state.ListState<PostEntry> negativeState;
        private transient org.apache.flink.api.common.state.ListState<AuthorEntry> positiveAuthorsState;
        private transient org.apache.flink.api.common.state.ListState<AuthorEntry> negativeAuthorsState;

        // 保留时长 12 小时（毫秒）
        private final long expireMs = 12L * 3600L * 1000L;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            org.apache.flink.api.common.state.ListStateDescriptor<PostEntry> dComplete =
                new org.apache.flink.api.common.state.ListStateDescriptor<>("completePlayState", PostEntry.class);
            completePlayState = getRuntimeContext().getListState(dComplete);
            stand10State = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("stand10State", PostScoreEntry.class));
            play8State = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("play8State", PostScoreEntry.class));
            likeState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("likeState", PostEntry.class));
            commentState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("commentState", PostEntry.class));
            shareState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("shareState", PostEntry.class));
            followState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("followState", PostEntry.class));
            collectState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("collectState", PostEntry.class));
            profileState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("profileState", PostEntry.class));
            payState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("payState", PostEntry.class));
            reportState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("reportState", PostEntry.class));
            dislikeState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("dislikeState", PostEntry.class));
            positiveState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("positiveState", PostEntry.class));
            negativeState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("negativeState", PostEntry.class));
            positiveAuthorsState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("positiveAuthorsState", AuthorEntry.class));
            negativeAuthorsState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("negativeAuthorsState", AuthorEntry.class));
        }

        @Override
        public void processElement(SessionSummary value, Context ctx, Collector<Tuple2<String, byte[]>> out) throws Exception {
            long now = ctx.timerService().currentProcessingTime();

            // 如果该 session summary 不满足任何触发特征，则跳过，避免无意义的 state 写入与 Redis 更新
            if (!isTriggeringSession(value)) {
                return;
            }

            // 先清理过期元素并将现有 state 转为 LinkedHashMap 以便按时间维持最近顺序
            LinkedHashMap<Long, PostEntry> completeMap = listStateToPostMap(completePlayState, now);
            
            LinkedHashMap<Long, PostScoreEntry> stand10Map = listStateToPostScoreMap(stand10State, now);
            LinkedHashMap<Long, PostScoreEntry> play8Map = listStateToPostScoreMap(play8State, now);
            LinkedHashMap<Long, PostEntry> likeMap = listStateToPostMap(likeState, now);
            LinkedHashMap<Long, PostEntry> commentMap = listStateToPostMap(commentState, now);
            LinkedHashMap<Long, PostEntry> shareMap = listStateToPostMap(shareState, now);
            LinkedHashMap<Long, PostEntry> followMap = listStateToPostMap(followState, now);
            LinkedHashMap<Long, PostEntry> collectMap = listStateToPostMap(collectState, now);
            LinkedHashMap<Long, PostEntry> profileMap = listStateToPostMap(profileState, now);
            LinkedHashMap<Long, PostEntry> payMap = listStateToPostMap(payState, now);
            LinkedHashMap<Long, PostEntry> reportMap = listStateToPostMap(reportState, now);
            LinkedHashMap<Long, PostEntry> dislikeMap = listStateToPostMap(dislikeState, now);
            LinkedHashMap<Long, PostEntry> positiveMap = listStateToPostMap(positiveState, now);
            LinkedHashMap<Long, PostEntry> negativeMap = listStateToPostMap(negativeState, now);
            LinkedHashMap<Long, AuthorEntry> positiveAuthorsMap = listStateToAuthorMap(positiveAuthorsState, now);
            LinkedHashMap<Long, AuthorEntry> negativeAuthorsMap = listStateToAuthorMap(negativeAuthorsState, now);

            // 根据 session summary 更新各个队列（保持与原聚合规则一致）
            if (value.isFullPlay) {
                addRecentPost(completeMap, value.postId, value.eventTime, 30);
            }
            
            if (value.standingTime > 10) {
                addRecentPostScore(stand10Map, value.postId, (float) value.standingTime, value.eventTime, 30);
            }
            if (value.playbackTime > 8) {
                addRecentPostScore(play8Map, value.postId, (float) value.playbackTime, value.eventTime, 30);
            }
            if (value.isLike) addRecentPost(likeMap, value.postId, value.eventTime, 10);
            if (value.isComment) addRecentPost(commentMap, value.postId, value.eventTime, 10);
            if (value.isShare) addRecentPost(shareMap, value.postId, value.eventTime, 10);
            if (value.isFollow) addRecentPost(followMap, value.postId, value.eventTime, 10);
            if (value.isFavor) addRecentPost(collectMap, value.postId, value.eventTime, 10);
            if (value.isProfile) addRecentPost(profileMap, value.postId, value.eventTime, 10);
            if (value.isPay) addRecentPost(payMap, value.postId, value.eventTime, 10);
            if (value.isReport) addRecentPost(reportMap, value.postId, value.eventTime, 10);
            if (value.isNotInterest) addRecentPost(dislikeMap, value.postId, value.eventTime, 10);

            // 正负反馈
            if (value.isLike || value.isComment || value.isShare || value.isFollow || value.isFavor || value.isProfile || value.isPay) {
                addRecentPost(positiveMap, value.postId, value.eventTime, 20);
                if (value.authorId > 0) addRecentAuthor(positiveAuthorsMap, value.authorId, value.eventTime, 20);
            }
            if (value.isReport || value.isNotInterest) {
                addRecentPost(negativeMap, value.postId, value.eventTime, 20);
                if (value.authorId > 0) addRecentAuthor(negativeAuthorsMap, value.authorId, value.eventTime, 20);
            }

            // 将更新后的 LinkedHashMap 写回 state（按最近顺序）
            writePostMapToListState(completePlayState, completeMap);
            
            writePostScoreMapToListState(stand10State, stand10Map);
            writePostScoreMapToListState(play8State, play8Map);
            writePostMapToListState(likeState, likeMap);
            writePostMapToListState(commentState, commentMap);
            writePostMapToListState(shareState, shareMap);
            writePostMapToListState(followState, followMap);
            writePostMapToListState(collectState, collectMap);
            writePostMapToListState(profileState, profileMap);
            writePostMapToListState(payState, payMap);
            writePostMapToListState(reportState, reportMap);
            writePostMapToListState(dislikeState, dislikeMap);
            writePostMapToListState(positiveState, positiveMap);
            writePostMapToListState(negativeState, negativeMap);
            writeAuthorMapToListState(positiveAuthorsState, positiveAuthorsMap);
            writeAuthorMapToListState(negativeAuthorsState, negativeAuthorsMap);

            // 构建用户特征对象并输出 protobuf bytes
            UserFeature24hAggregation agg = new UserFeature24hAggregation();
            agg.uid = value.uid;
            agg.completePlayPosts = completeMap;
            
            agg.stand10sPosts = stand10Map;
            agg.play8sPosts = play8Map;
            agg.likePosts = likeMap;
            agg.commentPosts = commentMap;
            agg.sharePosts = shareMap;
            agg.followPosts = followMap;
            agg.collectPosts = collectMap;
            agg.profilePosts = profileMap;
            agg.payPosts = payMap;
            agg.reportPosts = reportMap;
            agg.dislikePosts = dislikeMap;
            agg.positivePosts = positiveMap;
            agg.negativePosts = negativeMap;
            agg.positiveAuthors = positiveAuthorsMap;
            agg.negativeAuthors = negativeAuthorsMap;

            byte[] bytes = buildUserFeatureProto(agg);
            String redisKey = USER_PREFIX + ctx.getCurrentKey() + USER_SUFFIX;
            out.collect(new Tuple2<>(redisKey, bytes));
        }

        // 判断 session summary 是否满足触发条件（满足任一则触发）
        private boolean isTriggeringSession(SessionSummary value) {
            if (value == null) return false;
            // 基于现有聚合规则的触发条件：完播 / 停留10s以上 / 播放8s以上 / 以及显式行为反馈
            if (value.isFullPlay) return true;
            if (value.standingTime > 10) return true;
            if (value.playbackTime > 8) return true;
            if (value.isLike) return true;
            if (value.isComment) return true;
            if (value.isShare) return true;
            if (value.isFollow) return true;
            if (value.isFavor) return true;
            if (value.isProfile) return true;
            if (value.isPay) return true;
            if (value.isReport) return true;
            if (value.isNotInterest) return true;
            return false;
        }

        // 将 ListState 中非过期项转为 LinkedHashMap（按 insertion order）
        private LinkedHashMap<Long, PostEntry> listStateToPostMap(org.apache.flink.api.common.state.ListState<PostEntry> state, long now) throws Exception {
            LinkedHashMap<Long, PostEntry> map = new LinkedHashMap<>();
            if (state == null) return map;
            for (PostEntry p : state.get()) {
                if (p == null) continue;
                if (p.timestamp + expireMs < now) continue;
                map.put(p.postId, p);
            }
            return map;
        }

        private LinkedHashMap<Long, PostScoreEntry> listStateToPostScoreMap(org.apache.flink.api.common.state.ListState<PostScoreEntry> state, long now) throws Exception {
            LinkedHashMap<Long, PostScoreEntry> map = new LinkedHashMap<>();
            if (state == null) return map;
            for (PostScoreEntry p : state.get()) {
                if (p == null) continue;
                if (p.timestamp + expireMs < now) continue;
                map.put(p.postId, p);
            }
            return map;
        }

        private LinkedHashMap<Long, AuthorEntry> listStateToAuthorMap(org.apache.flink.api.common.state.ListState<AuthorEntry> state, long now) throws Exception {
            LinkedHashMap<Long, AuthorEntry> map = new LinkedHashMap<>();
            if (state == null) return map;
            for (AuthorEntry a : state.get()) {
                if (a == null) continue;
                if (a.timestamp + expireMs < now) continue;
                map.put(a.authorId, a);
            }
            return map;
        }

        private void writePostMapToListState(org.apache.flink.api.common.state.ListState<PostEntry> state, LinkedHashMap<Long, PostEntry> map) throws Exception {
            state.clear();
            for (PostEntry p : map.values()) {
                state.add(p);
            }
        }

        private void writePostScoreMapToListState(org.apache.flink.api.common.state.ListState<PostScoreEntry> state, LinkedHashMap<Long, PostScoreEntry> map) throws Exception {
            state.clear();
            for (PostScoreEntry p : map.values()) {
                state.add(p);
            }
        }

        private void writeAuthorMapToListState(org.apache.flink.api.common.state.ListState<AuthorEntry> state, LinkedHashMap<Long, AuthorEntry> map) throws Exception {
            state.clear();
            for (AuthorEntry a : map.values()) {
                state.add(a);
            }
        }
    }
    
    private static byte[] buildUserFeatureProto(UserFeature24hAggregation agg) {
        RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();
        
        // 使用最新的 \"latest\" 字段集合（proto lines 163-176）
        // 完播列表
        for (PostEntry p : agg.completePlayPosts.values()) {
            builder.addViewerCompleteplayPostsLatest(p.postId);
        }

        // 停留10s以上列表（IdScore）
        for (PostScoreEntry p : agg.stand10sPosts.values()) {
            builder.addViewer10SstandPostsLatest(
                RecFeature.IdScore.newBuilder()
                    .setId(p.postId)
                    .setScore((float)p.score)
                    .build()
            );
        }

        // 观看8s以上列表（IdScore）
        for (PostScoreEntry p : agg.play8sPosts.values()) {
            builder.addViewer8SplayPostsLatest(
                RecFeature.IdScore.newBuilder()
                    .setId(p.postId)
                    .setScore((float)p.score)
                    .build()
            );
        }

        // 点赞/评论/分享/收藏/付费/举报/不喜欢/正反馈/负反馈 列表
        for (PostEntry p : agg.likePosts.values()) {
            builder.addViewerLikePostsLatest(p.postId);
        }
        for (PostEntry p : agg.commentPosts.values()) {
            builder.addViewerCommentPostsLatest(p.postId);
        }
        for (PostEntry p : agg.sharePosts.values()) {
            builder.addViewerSharePostsLatest(p.postId);
        }
        for (PostEntry p : agg.collectPosts.values()) {
            builder.addViewerCollectPostsLatest(p.postId);
        }
        for (PostEntry p : agg.payPosts.values()) {
            builder.addViewerPayPostsLatest(p.postId);
        }
        for (PostEntry p : agg.reportPosts.values()) {
            builder.addViewerReportPostsLatest(p.postId);
        }
        for (PostEntry p : agg.dislikePosts.values()) {
            builder.addViewerDislikePostsLatest(p.postId);
        }
        for (PostEntry p : agg.positivePosts.values()) {
            builder.addViewerPositivePostsLatest(p.postId);
        }
        for (PostEntry p : agg.negativePosts.values()) {
            builder.addViewerNegetivePostsLatest(p.postId);
        }

        // 正/负反馈作者列表
        for (AuthorEntry a : agg.positiveAuthors.values()) {
            builder.addViewerPositiveAuthorsLatest(a.authorId);
        }
        for (AuthorEntry a : agg.negativeAuthors.values()) {
            builder.addViewerNegetiveAuthorsLatest(a.authorId);
        }
        
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
