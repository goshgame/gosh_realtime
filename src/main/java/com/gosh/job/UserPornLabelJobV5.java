package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.job.UserFeatureCommon.PostViewEvent;
import com.gosh.job.UserFeatureCommon.PostViewInfo;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.MySQLUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * UserPornLabelJobV5
 *
 * 目标：只维护“用户类型 + 试探/冷却状态机”的 Redis 状态，不在 Flink 作业里做任何“内容分发规则”。
 *
 * 你（线上系统）将通过查询本作业写入的多个 Redis Key 来决定：
 * - 不同用户类型（有专区/无专区/关闭专区）
 * - 当前试探阶段（Mid试探/High试探/稳定/冷却）
 * - 冷却期结束时间点（结合印度时间20:00规则）
 *
 * 说明：
 * - 本作业复用 V3 的“正反馈/负反馈”判定（基于最近 N 条观看事件聚合）
 * - 本作业不再写入 V3 的 rtylevel_v3 / meta 等 key，避免干扰旧逻辑
 * - 用户类型判定留“可插拔接口”，默认优先读线上提前写入的 Redis key（可替换为 MySQL 查询）
 */
public class UserPornLabelJobV5 {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJobV5.class);

    // 监控白名单用户（只对这些 uid 打详细日志）
    private static final Set<Long> MONITORED_UIDS = new HashSet<>(Arrays.asList(
            110191L, 10000086L, 10000214L, 10000970L, 10001052L, 10001063L,
            10284668L, 10287287L, 10287385L, 10321482L, 10323172L, 10327349L,
            10385628L, 10509837L, 10585140L, 10636698L, 10638032L, 10771311L,
            11043734L, 11075974L, 11487816L, 11638815L, 11675486L, 11748695L,
            11768495L, 11933458L, 12103877L, 12214871L, 12217124L, 12255049L,
            12576879L, 12710246L, 12887277L, 13120233L, 13131686L, 13149764L,
            13180151L, 13242700L, 13337443L, 13372756L, 13374748L, 13659161L,
            13661418L, 13674458L, 13747623L, 13772042L, 13772354L, 13777984L,
            13794682L, 14550456L, 14601257L
    ));

    private static boolean isMonitored(long uid) {
        return MONITORED_UIDS.contains(uid);
    }

    // =========================
    // 1) V5 Redis Keys（拆分存储，便于线上按 key 做分发）
    // =========================
    // 用户类型（由线上系统/其他作业写入；本作业也会尝试填充默认值）
    private static final String REDIS_KEY_USER_TYPE_V5 = "rec_post:{%d}:porn_user_type_v5";

    // 试探/冷却阶段
    private static final String REDIS_KEY_STAGE_V5 = "rec_post:{%d}:porn_stage_v5";
    // 试探次数（用于递增冷却期：3d -> 7d -> 14d）
    private static final String REDIS_KEY_PROBE_COUNT_V5 = "rec_post:{%d}:porn_probe_count_v5";
    // 冷却结束时间戳（ms），已对齐“印度时间20:00后可重新试探”的规则
    private static final String REDIS_KEY_COOLDOWN_END_TS_V5 = "rec_post:{%d}:porn_cooldown_end_ts_v5";
    // 冷却档位（1/2/3，对应 3d/7d/14d；关闭专区用户可走特殊档位）
    private static final String REDIS_KEY_COOLDOWN_STAGE_V5 = "rec_post:{%d}:porn_cooldown_stage_v5";

    // 最近一次反馈（用于线上调试/回溯）
    private static final String REDIS_KEY_LAST_FEEDBACK_V5 = "rec_post:{%d}:porn_last_feedback_v5"; // positive/negative/none
    private static final String REDIS_KEY_LAST_FEEDBACK_TS_V5 = "rec_post:{%d}:porn_last_feedback_ts_v5"; // ms
    private static final String REDIS_KEY_LAST_POS_TAG_V5 = "rec_post:{%d}:porn_last_pos_tag_v5";
    private static final String REDIS_KEY_LAST_NEG_TAG_V5 = "rec_post:{%d}:porn_last_neg_tag_v5";
    private static final String REDIS_KEY_LAST_UPDATE_TS_V5 = "rec_post:{%d}:porn_last_update_ts_v5";

    // Flow 汇总 Key：把完整状态压缩到一个 value 中，便于线上“一眼看懂用户在流程图中的位置”
    // 值示例：
    // v5;user_type=no_zone_user;stage=probing_mid;dist=mid2_high0;probe_round=1;cooling_stage=1;cooling_end_ts=1672531200000;
    //    last_feedback=positive;last_feedback_ts=1672444800000;last_pos_tag=mid;last_neg_tag=unk;last_update_ts=1672444800000
    private static final String REDIS_KEY_FLOW_V5 = "rec_post:{%d}:porn_state_flow_v5";

    // 冗余分发模式 Key：只记录“最多出几个 mid / 几个 high”，方便线上直接使用
    // 约定取值：
    // - "mid2_high0"   : 最多出 2 个 mid（P1/P2 试探阶段）
    // - "mid1_high1"   : 最多出 1 个 high + 1 个 mid（P3 试探阶段）
    // - "mid0_high2"   : 最多出 2 个 high（稳定期 P4，可按需理解为“强偏好”）
    // - "mid1_high0"   : 冷却期，仅最多 1 个 mid（C1/C2/C3）
    private static final String REDIS_KEY_DIST_V5 = "rec_post:{%d}:porn_dist_v5";

    // =========================
    // 2) TTL 策略
    // =========================
    // 注意：RedisSink 只支持“全流固定 TTL”，无法逐条动态 TTL。
    // 因此 V5 采用“写绝对时间戳（cooldown_end_ts）+ 较长 TTL”的组合：
    // - 冷却结束逻辑由线上系统或本作业通过 timestamp 判定
    // - TTL 仅作为兜底清理（避免永不过期）
    private static final int REDIS_TTL_V5 = 30 * 24 * 3600; // 30天

    // Kafka Group ID（避免与 V3 冲突）
    private static final String KAFKA_GROUP_ID = "rec_porn_state_v5";

    // 印度时区（用于“20:00后可重新试探”的规则）
    private static final ZoneId INDIA_ZONE = ZoneId.of("Asia/Kolkata");

    // =========================
    // 3) 用户类型 / 试探阶段定义
    // =========================
    public enum UserTypeV5 {
        ZONE_USER,       // 有专区用户
        NO_ZONE_USER,    // 历史无创建过专区用户
        BLOCK_ZONE_USER, // 关闭专区用户
        UNKNOWN
    }

    public enum StageV5 {
        PROBING_MID,   // 试探：从Mid开始
        PROBING_HIGH,  // 试探：High阶段
        STABLE,        // 稳定：已有明确偏好
        COOLING        // 冷却：等待冷却结束再试探
    }

    public enum FeedbackV5 {
        POSITIVE, NEGATIVE, NONE
    }

    // =========================
    // 4) 埋点事件定义（本作业内独立实现，不再依赖其他 Job）
    // =========================

    // 事件类型常量（与 RecUserPostFeatureLatestJob 保持一致）
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
    private static final String EVENT_PROMPT_NO = "prompt_recommendation_click_no";

    /**
     * Post 埋点事件基础类（与 RecUserPostFeatureLatestJob.PostEvent 对齐）
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
     * 事件解析器：从 event_post Kafka 日志解析出所有 Post 相关事件
     */
    public static class PostEventParser implements FlatMapFunction<String, PostEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String value, Collector<PostEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                return;
            }
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);

                // 获取 user_event_log 对象
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }

                // 获取事件类型
                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();

                // 只处理 post 相关事件
                if (!isPostEvent(event)) {
                    return;
                }

                // 获取 event_data 字段
                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }

                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equalsIgnoreCase(eventData)) {
                    return;
                }

                // 解析 event_data 的 JSON 内容
                JsonNode data = OBJECT_MAPPER.readTree(eventData);

                // 提取时间戳
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                }

                // 根据不同事件类型解析（PostEventParser 内部仍按 data.userId 取 userId）
                parseEvent(event, data, eventTimeMillis, out);

            } catch (Exception e) {
                // 静默处理异常，避免影响主流程
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
                evt.authorId = 0; // 曝光事件没有 author_id
                evt.recToken = recToken;
                evt.exposedPos = exposedPos;
                evt.eventTime = eventTime;
                evt.postLength = post.path("length").asDouble(0);

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
            parseSimpleAction(data, eventTime, EVENT_POST_COMMENT, out, (evt) -> evt.isComment = true);
        }

        private void parsePostShare(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_SHARE, out, (evt) -> evt.isShare = true);
        }

        private void parsePostFollow(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_FOLLOW, out, (evt) -> evt.isFollow = true);
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
            parseSimpleAction(data, eventTime, EVENT_POST_CLICK_NICKNAME, out, (evt) -> evt.isProfile = true);
        }

        private void parsePostReport(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_REPORT, out, (evt) -> evt.isReport = true);
        }

        private void parsePostUninterested(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_UNINTERESTED, out, (evt) -> evt.isNotInterest = true);
        }

        private void parsePostPaid(JsonNode data, long eventTime, Collector<PostEvent> out) {
            parseSimpleAction(data, eventTime, EVENT_POST_PAID, out, (evt) -> evt.isPay = true);
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

    /**
     * 从埋点 JSON 中抽取用户 ID 的辅助方法：
     * 1. 优先从 user_event_log.uid 获取（与你提供的示例结构保持一致）
     * 2. 兜底从 event_data.userId 获取（兼容其他埋点格式，支持字符串或数值）
     */
    private static long extractUserId(JsonNode userEventLog, JsonNode data) {
        if (userEventLog != null && !userEventLog.isMissingNode()) {
            JsonNode uidNode = userEventLog.path("uid");
            if (!uidNode.isMissingNode()) {
                long uidVal = uidNode.asLong(0);
                if (uidVal > 0) {
                    return uidVal;
                }
            }
        }
        if (data == null || data.isMissingNode()) {
            return 0;
        }
        JsonNode userIdNode = data.path("userId");
        if (userIdNode.isMissingNode()) {
            return 0;
        }
        if (userIdNode.isTextual()) {
            try {
                return Long.parseLong(userIdNode.asText("0"));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return userIdNode.asLong(0);
    }

    // =========================
    // 4) 可插拔用户类型判定接口（留给你接线上 Redis/MySQL）
    // =========================
    public interface UserTypeResolver extends Serializable {
        UserTypeV5 resolve(long uid, RedisConnectionManager redis);
    }

    /**
     * 默认实现：优先从线上 Redis 读“源用户类型”。
     *
     * 你可以在推荐线上系统或其它离线/实时作业写入这个 key（字符串）：
     * - "zone_user" / "no_zone_user" / "block_zone_user"
     *
     * 如果不存在或值不合法，则返回 UNKNOWN（后续你可替换为 MySQL 查询）。
     */
    public static class RedisFirstUserTypeResolver implements UserTypeResolver {
        // 预留：线上系统可以维护此 key，供本作业读取
        private static final String REDIS_KEY_USER_TYPE_SOURCE = "rec_post:{%d}:porn_user_type_source";

        @Override
        public UserTypeV5 resolve(long uid, RedisConnectionManager redis) {
            try {
                String key = String.format(REDIS_KEY_USER_TYPE_SOURCE, uid);
                Tuple2<String, byte[]> tuple = redis.getStringCommands().get(key);
                if (tuple == null || tuple.f1 == null || tuple.f1.length == 0) {
                    return UserTypeV5.UNKNOWN;
                }
                String v = new String(tuple.f1, StandardCharsets.UTF_8).trim().toLowerCase(Locale.ROOT);
                if ("zone_user".equals(v)) return UserTypeV5.ZONE_USER;
                if ("no_zone_user".equals(v)) return UserTypeV5.NO_ZONE_USER;
                if ("block_zone_user".equals(v) || "closed_zone_user".equals(v) || "closed_zone".equals(v)) return UserTypeV5.BLOCK_ZONE_USER;
                return UserTypeV5.UNKNOWN;
            } catch (Exception e) {
                return UserTypeV5.UNKNOWN;
            }
        }
    }

    /**
     * MySQL 用户类型解析器（带本地缓存，1小时 TTL）
     *
     * 从 MySQL 的 post_user_setting 表查询 zone_mode_type：
     * - zone_mode_type=1 → ZONE_USER（有专区）
     * - zone_mode_type=2 → BLOCK_ZONE_USER（关闭专区）
     * - zone_mode_type=0 或不在表中 → NO_ZONE_USER（未创建专区）
     */
    public static class MySQLUserTypeResolver implements UserTypeResolver {
        private static final String DS_NAME = "db2";  // 数据源名称
        private static final String DB_NAME = "gosh_social";  // 数据库名称
        private static final String QUERY_SQL = "SELECT zone_mode_type FROM post_user_setting WHERE uid = ? LIMIT 1";
        private static final long CACHE_TTL_MS = 3600 * 1000L;  // 1小时缓存

        // 本地缓存：uid -> (userType, expireTime)
        private final Map<Long, CacheEntry> cache = new ConcurrentHashMap<>();
        private static final int MAX_CACHE_SIZE = 100000;  // 最大缓存条目数（防止内存溢出）
        private volatile long lastCleanupTime = 0L;
        private static final long CLEANUP_INTERVAL_MS = 300000L;  // 每5分钟清理一次过期缓存

        @Override
        public UserTypeV5 resolve(long uid, RedisConnectionManager redis) {
            long now = System.currentTimeMillis();
            
            // 1. 先查缓存
            CacheEntry cached = cache.get(uid);
            if (cached != null && cached.expireTime > now) {
                if (isMonitored(uid)) {
                    LOG.info("[PornStateV5][MySQLUserType] cache hit: uid={}, userType={}", uid, cached.userType);
                }
                // 缓存命中，定期清理过期缓存（避免频繁清理）
                if (now - lastCleanupTime > CLEANUP_INTERVAL_MS) {
                    cleanupExpiredCache(now);
                    lastCleanupTime = now;
                }
                return cached.userType;
            }

            // 2. 缓存未命中或过期，查询 MySQL
            UserTypeV5 userType = queryFromMySQL(uid);
            if (isMonitored(uid)) {
                LOG.info("[PornStateV5][MySQLUserType] mysql query: uid={}, userType={}", uid, userType);
            }

            // 3. 更新缓存（如果缓存未满或已过期可替换）
            if (cache.size() < MAX_CACHE_SIZE || cached != null) {
                cache.put(uid, new CacheEntry(userType, now + CACHE_TTL_MS));
            } else if (cache.size() >= MAX_CACHE_SIZE) {
                // 缓存已满且不是替换过期项，先清理过期缓存再尝试添加
                cleanupExpiredCache(now);
                if (cache.size() < MAX_CACHE_SIZE) {
                    cache.put(uid, new CacheEntry(userType, now + CACHE_TTL_MS));
                }
            }

            // 4. 定期清理过期缓存
            if (now - lastCleanupTime > CLEANUP_INTERVAL_MS) {
                cleanupExpiredCache(now);
                lastCleanupTime = now;
            }

            return userType;
        }

        /**
         * 从 MySQL 查询用户专区类型
         */
        private UserTypeV5 queryFromMySQL(long uid) {
            try (Connection conn = MySQLUtil.getConnection(DS_NAME, DB_NAME);
                 PreparedStatement stmt = conn.prepareStatement(QUERY_SQL)) {
                
                stmt.setLong(1, uid);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int zoneModeType = rs.getInt("zone_mode_type");
                        if (isMonitored(uid)) {
                            LOG.info("[PornStateV5][MySQLUserType] mysql row: uid={}, zone_mode_type={}", uid, zoneModeType);
                        }
                        // zone_mode_type=1 → ZONE_USER（有专区）
                        if (zoneModeType == 1) {
                            return UserTypeV5.ZONE_USER;
                        }
                        // zone_mode_type=2 → BLOCK_ZONE_USER（关闭专区）
                        if (zoneModeType == 2) {
                            return UserTypeV5.BLOCK_ZONE_USER;
                        }
                        // zone_mode_type=0 或其他值 → NO_ZONE_USER（未创建专区）
                        return UserTypeV5.NO_ZONE_USER;
                    } else {
                        // 不在表中 → NO_ZONE_USER（未创建专区）
                        return UserTypeV5.NO_ZONE_USER;
                    }
                }
            } catch (SQLException | ClassNotFoundException e) {
                LOG.warn("[MySQLUserTypeResolver] 查询用户专区类型失败, uid={}, err={}", uid, e.getMessage());
                // 查询失败时返回 UNKNOWN，让上层降级处理
                return UserTypeV5.UNKNOWN;
            }
        }

        /**
         * 清理过期缓存
         */
        private void cleanupExpiredCache(long now) {
            cache.entrySet().removeIf(entry -> entry.getValue().expireTime <= now);
        }

        /**
         * 缓存条目
         */
        private static class CacheEntry implements Serializable {
            final UserTypeV5 userType;
            final long expireTime;

            CacheEntry(UserTypeV5 userType, long expireTime) {
                this.userType = userType;
                this.expireTime = expireTime;
            }
        }
    }

    // =========================
    // 5) Flink Job 主流程（复用 V3 的 RecentNExposures + 聚合逻辑）
    // =========================
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

        Properties kafkaProperties = KafkaEnvUtil.loadProperties();
        kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);
        // 使用与 RecUserPostFeatureLatestJob 相同的埋点 topic：event_post
        KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(kafkaProperties, "event_post");

        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 只保留 event_type = 1 的埋点事件
        DataStream<String> filteredStream = kafkaStream
                .filter(EventFilterUtil.createFastEventTypeFilter(1))
                .name("Pre-filter Post Events (event_type=1)");

        // 解析事件埋点为 PostEvent（本文件内定义）
        SingleOutputStreamOperator<PostEvent> basePostEventStream = filteredStream
                .flatMap(new PostEventParser())
                .name("Parse Post Events (base)");

        // 额外解析 prompt_recommendation_click_no 试探弹窗否定事件，补充为负反馈
        SingleOutputStreamOperator<PostEvent> promptNoStream = filteredStream
                .flatMap(new PromptNoEventParser())
                .name("Parse Prompt Recommendation No Events");

        // 合并两路 PostEvent 流
        DataStream<PostEvent> postEventStream = basePostEventStream
                .union(promptNoStream);

        // 将 PostEvent 映射为 V5 内部使用的 PostViewEvent/PostViewInfo，便于复用 V3 的聚合与判定逻辑
        SingleOutputStreamOperator<PostViewEvent> viewStream = postEventStream
                .flatMap(new PostEventToViewEventMapper())
                .name("PostEvent -> PostViewEvent Mapper");

        SingleOutputStreamOperator<UserNExposures> recentStats = viewStream
                .keyBy(event -> event.uid)
                .process(new RecentNExposures())
                .name("recent-exposure-statistics-v5");

        // 使用 MySQL 用户类型解析器（带1小时本地缓存）
        DataStream<StateUpdateResult> updateResultStream = recentStats
                .map(new PornStateCalculatorV5(new MySQLUserTypeResolver()))
                .name("calc-porn-state-v5");

        DataStream<Tuple2<String, byte[]>> kvStream = updateResultStream
                .filter(r -> r != null && r.kvPairs != null && !r.kvPairs.isEmpty())
                .flatMap((StateUpdateResult r, Collector<Tuple2<String, byte[]>> out) -> {
                    for (Tuple2<String, byte[]> kv : r.kvPairs) {
                        if (kv != null && kv.f0 != null && kv.f1 != null) {
                            out.collect(kv);
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, byte[]>>() {}))
                .name("porn-state-v5-kv-stream");

        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(REDIS_TTL_V5);
        RedisUtil.addRedisSink(kvStream, redisConfig, true, 200, 10, 5000);

        env.execute("UserPornLabelJobV5");
    }

    // =========================
    // 6) V3 复用的数据结构（按需拷贝，避免改动 V3 文件）
    // =========================
    public static class PostBehaviorDetail implements Serializable {
        public long postId;
        public float standingTime;
        public float progressTime;
        public List<String> positiveActions = new ArrayList<>();
        public List<String> negativeActions = new ArrayList<>();

        public PostBehaviorDetail(long postId, float standingTime, float progressTime) {
            this.postId = postId;
            this.standingTime = standingTime;
            this.progressTime = progressTime;
        }
    }

    public static class TagStatistics implements Serializable {
        public float standingTime = 0.0f;
        public int shortPlayPostCount = 0;
        public int positiveCount = 0;
        public int negativeCount = 0;
        public int dislikeCount = 0;
        public long triggerPostId = 0;
        public List<PostBehaviorDetail> allPostDetails = new ArrayList<>();
        public List<PostBehaviorDetail> longPlayPostDetails = new ArrayList<>();
        public List<PostBehaviorDetail> positivePostDetails = new ArrayList<>();
        public List<PostBehaviorDetail> negativePostDetails = new ArrayList<>();
    }

    public static class StateUpdateResult implements Serializable {
        public long uid;
        public List<Tuple2<String, byte[]>> kvPairs = new ArrayList<>();
    }

    /**
     * 计算“正/负反馈”并更新 V5 的用户状态 key（只维护状态，不管分发）
     */
    public static class PornStateCalculatorV5 extends RichMapFunction<UserNExposures, StateUpdateResult> {
        private final UserTypeResolver userTypeResolver;
        private transient RedisConnectionManager redisManager;

        public PornStateCalculatorV5(UserTypeResolver userTypeResolver) {
            this.userTypeResolver = userTypeResolver;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
        }

        @Override
        public StateUpdateResult map(UserNExposures event) {
            long uid = event.viewer;
            boolean monitored = isMonitored(uid);

            Map<String, TagStatistics> statsMap = aggregateStats(event);
            Tuple3<String, Long, TagStatistics> pos = pickHighestPositive(statsMap);
            Tuple3<String, Long, TagStatistics> neg = pickLowestNegative(statsMap);

            String posTag = pos.f0;
            String negTag = neg.f0;

            FeedbackV5 feedback = decideFeedback(posTag, negTag);
            if (feedback == FeedbackV5.NONE) {
                // 没有明确反馈信号，不刷 Redis，避免写放大
                if (monitored) {
                    LOG.info("[PornStateV5][Feedback] uid={} no feedback (NONE), posTag={}, negTag={}", uid, posTag, negTag);
                }
                return emptyResult(uid);
            }

            long nowMs = System.currentTimeMillis();

            // 1) 用户类型：优先读本作业维护的 key；若不存在则调用 resolver（预留线上接口）
            UserTypeV5 userType = readUserType(uid);
            if (userType == UserTypeV5.UNKNOWN) {
                userType = userTypeResolver != null ? userTypeResolver.resolve(uid, redisManager) : UserTypeV5.UNKNOWN;
            }

            // 2) 读取当前状态
            StageV5 stage = readStage(uid);
            int probeCount = readInt(uid, REDIS_KEY_PROBE_COUNT_V5, 0);
            long cooldownEndTs = readLong(uid, REDIS_KEY_COOLDOWN_END_TS_V5, 0L);
            int cooldownStage = readInt(uid, REDIS_KEY_COOLDOWN_STAGE_V5, 0);

            if (monitored) {
                LOG.info("[PornStateV5][BeforeTransition] uid={}, feedback={}, posTag={}, negTag={}, userType={}, stage={}, probeCount={}, cooldownStage={}, cooldownEndTs={}",
                        uid, feedback, posTag, negTag, userType, stage, probeCount, cooldownStage, cooldownEndTs);
            }

            // 3) 冷却是否到期（注意“印度20:00后可重新试探”已被对齐到 cooldownEndTs）
            boolean coolingExpired = (cooldownEndTs > 0 && nowMs >= cooldownEndTs);
            if (stage == StageV5.COOLING && coolingExpired) {
                stage = StageV5.PROBING_MID;
                cooldownEndTs = 0L;
                cooldownStage = 0;
                if (monitored) {
                    LOG.info("[PornStateV5][CoolingExpired] uid={} cooling expired, reset to PROBING_MID", uid);
                }
            }

            // 4) 状态转移（只维护状态，不做分发）
            TransitionResult tr = transition(userType, stage, probeCount, cooldownStage, cooldownEndTs, feedback, nowMs);

            if (monitored) {
                LOG.info("[PornStateV5][AfterTransition] uid={}, newUserType={}, newStage={}, newProbeCount={}, newCooldownStage={}, newCooldownEndTs={}",
                        uid, tr.userType, tr.stage, tr.probeCount, tr.cooldownStage, tr.cooldownEndTs);
            }

            // 5) 生成写入
            StateUpdateResult out = new StateUpdateResult();
            out.uid = uid;

            // 用户类型
            out.kvPairs.add(kv(String.format(REDIS_KEY_USER_TYPE_V5, uid), tr.userType.name().toLowerCase(Locale.ROOT)));
            // 阶段
            out.kvPairs.add(kv(String.format(REDIS_KEY_STAGE_V5, uid), tr.stage.name().toLowerCase(Locale.ROOT)));
            // 冗余分发模式（区分“最多几个 mid / 几个 high”）
            String distValue = computeDistValue(tr.stage);
            out.kvPairs.add(kv(String.format(REDIS_KEY_DIST_V5, uid), distValue));
            // 试探次数/冷却
            out.kvPairs.add(kv(String.format(REDIS_KEY_PROBE_COUNT_V5, uid), String.valueOf(tr.probeCount)));
            out.kvPairs.add(kv(String.format(REDIS_KEY_COOLDOWN_STAGE_V5, uid), String.valueOf(tr.cooldownStage)));
            out.kvPairs.add(kv(String.format(REDIS_KEY_COOLDOWN_END_TS_V5, uid), String.valueOf(tr.cooldownEndTs)));

            // 最近一次反馈
            out.kvPairs.add(kv(String.format(REDIS_KEY_LAST_FEEDBACK_V5, uid), feedback.name().toLowerCase(Locale.ROOT)));
            out.kvPairs.add(kv(String.format(REDIS_KEY_LAST_FEEDBACK_TS_V5, uid), String.valueOf(nowMs)));
            out.kvPairs.add(kv(String.format(REDIS_KEY_LAST_POS_TAG_V5, uid), posTag == null ? "unk" : posTag));
            out.kvPairs.add(kv(String.format(REDIS_KEY_LAST_NEG_TAG_V5, uid), negTag == null ? "unk" : negTag));
            out.kvPairs.add(kv(String.format(REDIS_KEY_LAST_UPDATE_TS_V5, uid), String.valueOf(nowMs)));

            // Flow 汇总 value：把所有关键字段都串在一起，便于线上/排查快速定位状态
            String flowValue = String.format(
                    Locale.ROOT,
                    "v5;user_type=%s;stage=%s;dist=%s;probe_round=%d;cooling_stage=%d;cooling_end_ts=%d;"
                            + "last_feedback=%s;last_feedback_ts=%d;last_pos_tag=%s;last_neg_tag=%s;last_update_ts=%d",
                    tr.userType.name().toLowerCase(Locale.ROOT),
                    tr.stage.name().toLowerCase(Locale.ROOT),
                    distValue,
                    tr.probeCount,
                    tr.cooldownStage,
                    tr.cooldownEndTs,
                    feedback.name().toLowerCase(Locale.ROOT),
                    nowMs,
                    posTag == null ? "unk" : posTag,
                    negTag == null ? "unk" : negTag,
                    nowMs
            );
            out.kvPairs.add(kv(String.format(REDIS_KEY_FLOW_V5, uid), flowValue));

            if (monitored) {
                LOG.info("[PornStateV5][RedisWrite] uid={}, keys_written=[user_type_v5, stage_v5, dist_v5, probe_count_v5, cooldown_stage_v5, cooldown_end_ts_v5, last_feedback_v5, last_feedback_ts_v5, last_pos_tag_v5, last_neg_tag_v5, last_update_ts_v5, flow_v5], flow={}",
                        uid, flowValue);
            }

            return out;
        }

        private StateUpdateResult emptyResult(long uid) {
            StateUpdateResult r = new StateUpdateResult();
            r.uid = uid;
            r.kvPairs = Collections.emptyList();
            return r;
        }

        private FeedbackV5 decideFeedback(String posTag, String negTag) {
            // 约定：负反馈优先（更保守），避免“同时有正/负”时过于激进升级
            if (negTag != null && !"unk".equals(negTag) && !"clean".equals(negTag)) {
                return FeedbackV5.NEGATIVE;
            }
            if (posTag != null && !"unk".equals(posTag) && !"clean".equals(posTag)) {
                return FeedbackV5.POSITIVE;
            }
            return FeedbackV5.NONE;
        }

        private UserTypeV5 readUserType(long uid) {
            String v = readString(String.format(REDIS_KEY_USER_TYPE_V5, uid));
            if (v == null) return UserTypeV5.UNKNOWN;
            String s = v.trim().toLowerCase(Locale.ROOT);
            if ("zone_user".equals(s) || "zone".equals(s)) return UserTypeV5.ZONE_USER;
            if ("no_zone_user".equals(s) || "no_zone".equals(s)) return UserTypeV5.NO_ZONE_USER;
            if ("block_zone_user".equals(s) || "closed_zone_user".equals(s) || "closed_zone".equals(s)) return UserTypeV5.BLOCK_ZONE_USER;
            return UserTypeV5.UNKNOWN;
        }

        private StageV5 readStage(long uid) {
            String v = readString(String.format(REDIS_KEY_STAGE_V5, uid));
            if (v == null) return StageV5.PROBING_MID; // 默认：从Mid试探开始
            String s = v.trim().toLowerCase(Locale.ROOT);
            if ("probing_mid".equals(s)) return StageV5.PROBING_MID;
            if ("probing_high".equals(s)) return StageV5.PROBING_HIGH;
            if ("stable".equals(s)) return StageV5.STABLE;
            if ("cooling".equals(s)) return StageV5.COOLING;
            return StageV5.PROBING_MID;
        }

        private int readInt(long uid, String keyFmt, int def) {
            String v = readString(String.format(keyFmt, uid));
            if (v == null) return def;
            try {
                return Integer.parseInt(v.trim());
            } catch (Exception e) {
                return def;
            }
        }

        private long readLong(long uid, String keyFmt, long def) {
            String v = readString(String.format(keyFmt, uid));
            if (v == null) return def;
            try {
                return Long.parseLong(v.trim());
            } catch (Exception e) {
                return def;
            }
        }

        private String readString(String key) {
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(key);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    return new String(tuple.f1, StandardCharsets.UTF_8);
                }
            } catch (Exception ignored) {
            }
            return null;
        }

        private Tuple2<String, byte[]> kv(String key, String value) {
            return Tuple2.of(key, value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * 根据阶段映射到“最多几个 mid / 几个 high”的分发模式。
         *
         * 约定：
         * - PROBING_MID : "mid2_high0"   （最多 2 个 mid）
         * - PROBING_HIGH: "mid1_high1"   （最多 1 个 high + 1 个 mid）
         * - STABLE      : "mid0_high2"   （最多 2 个 high，可理解为强偏好稳定期）
         * - COOLING     : "mid1_high0"   （冷却期，仅 1 个 mid 做保守唤醒）
         */
        private String computeDistValue(StageV5 stage) {
            switch (stage) {
                case PROBING_MID:
                    return "mid2_high0";
                case PROBING_HIGH:
                    return "mid1_high1";
                case STABLE:
                    return "mid0_high2";
                case COOLING:
                default:
                    return "mid1_high0";
            }
        }

        private TransitionResult transition(
                UserTypeV5 userType,
                StageV5 stage,
                int probeCount,
                int cooldownStage,
                long cooldownEndTs,
                FeedbackV5 feedback,
                long nowMs
        ) {
            // 默认回填 userType（UNKNOWN 也允许写入，便于线上发现未判定用户）
            TransitionResult r = new TransitionResult();
            r.userType = userType;
            r.stage = stage;
            r.probeCount = probeCount;
            r.cooldownStage = cooldownStage;
            r.cooldownEndTs = cooldownEndTs;

            // 有专区用户：状态机只维护为 stable（不在这里做“内容分发”，线上可依据 user_type）
            if (userType == UserTypeV5.ZONE_USER) {
                r.stage = StageV5.STABLE;
                r.cooldownEndTs = 0L;
                r.cooldownStage = 0;
                // probeCount 不强行清零（保留历史），但也可以清零；这里选择清零，避免冷却历史误导线上
                r.probeCount = 0;
                return r;
            }

            // 冷却期内：不改变冷却结束时间；仅记录反馈由外层写入 last_feedback*
            if (stage == StageV5.COOLING) {
                // 若外层已处理冷却到期，stage 不会是 COOLING
                return r;
            }

            // ===== 试探/稳定阶段的转移 =====
            switch (stage) {
                case PROBING_MID:
                    if (feedback == FeedbackV5.POSITIVE) {
                        r.stage = StageV5.PROBING_HIGH;
                        return r;
                    }
                    if (feedback == FeedbackV5.NEGATIVE) {
                        // 选择 B：只有“完成一轮试探并进入冷却”才递增 probe_count
                        return enterCooling(userType, probeCount, nowMs, 1, true);
                    }
                    return r;
                case PROBING_HIGH:
                    if (feedback == FeedbackV5.POSITIVE) {
                        r.stage = StageV5.STABLE;
                        r.probeCount = 0;
                        r.cooldownStage = 0;
                        r.cooldownEndTs = 0L;
                        return r;
                    }
                    if (feedback == FeedbackV5.NEGATIVE) {
                        // 选择 B：只有“完成一轮试探并进入冷却”才递增 probe_count
                        return enterCooling(userType, probeCount, nowMs, 2, true);
                    }
                    return r;
                case STABLE:
                    if (feedback == FeedbackV5.NEGATIVE) {
                        // 稳定期的负反馈：直接进入更长冷却（档位=3 -> 14d）
                        // 选择 B：stable 不是“试探阶段”，不递增 probe_count，只更新冷却信息
                        return enterCooling(userType, Math.max(probeCount, 2), nowMs, 3, false);
                    }
                    return r;
                default:
                    return r;
            }
        }

        /**
         * 进入冷却阶段
         *
         * @param incrementProbeCount 是否递增 probe_count（B：仅在试探阶段进入冷却时递增）
         */
        private TransitionResult enterCooling(UserTypeV5 userType, int probeCount, long nowMs, int expectedStage, boolean incrementProbeCount) {
            TransitionResult r = new TransitionResult();
            r.userType = userType;
            r.stage = StageV5.COOLING;

            // 冷却档位：按 expectedStage（1/2/3）推进
            // probe_count：B 模式下仅在“试探阶段进入冷却”才递增；stable->cooling 不递增
            int newProbeCount = incrementProbeCount ? (probeCount + 1) : probeCount;
            r.probeCount = newProbeCount;
            if (incrementProbeCount) {
                // 试探阶段进入冷却：允许用累计 probe_count 推进冷却档位（递增冷却）
                r.cooldownStage = Math.max(expectedStage, newProbeCount);
            } else {
                // 稳定期进入冷却：强制固定冷却档位为 expectedStage（你要求：stable->cooling 固定为 3）
                r.cooldownStage = expectedStage;
            }

            long rawEnd;
            // 无 close_time 字段时：统一以 nowMs 为起点计算冷却
            // - no_zone_user: 3/7/14 递增
            // - block_zone_user: 1/3/7 递增（你确认的规则）
            int days = cooldownDaysByStage(userType, r.cooldownStage);
            rawEnd = nowMs + days * 24L * 3600L * 1000L;

            // 关键：对齐到“印度时间 20:00 后可重新试探”
            long aligned = alignToNextIndia20(rawEnd);
            r.cooldownEndTs = aligned;
            return r;
        }

        private int cooldownDaysByStage(UserTypeV5 userType, int stage) {
            // 递增档位约定：
            // - no_zone_user: 3 / 7 / 14
            // - block_zone_user: 1 / 3 / 7 （你确认）
            int s = stage <= 1 ? 1 : (stage == 2 ? 2 : 3);
            if (userType == UserTypeV5.BLOCK_ZONE_USER) {
                if (s == 1) return 1;
                if (s == 2) return 3;
                return 7;
            }
            // 默认：no_zone_user / 其他
            if (s == 1) return 3;
            if (s == 2) return 7;
            return 14;
        }

        /**
         * 将一个时间戳对齐到“印度时间 20:00 的下一次到达点（含当天）”
         * - 如果 ts 对应的印度时间 < 20:00：对齐到当天 20:00
         * - 否则：对齐到次日 20:00
         */
        private long alignToNextIndia20(long tsMs) {
            ZonedDateTime zdt = Instant.ofEpochMilli(tsMs).atZone(INDIA_ZONE);
            ZonedDateTime today20 = zdt.withHour(20).withMinute(0).withSecond(0).withNano(0);
            ZonedDateTime target = zdt.isBefore(today20) ? today20 : today20.plusDays(1);
            return target.toInstant().toEpochMilli();
        }

        // ====== 状态载体 ======
        private static class TransitionResult {
            UserTypeV5 userType;
            StageV5 stage;
            int probeCount;
            int cooldownStage;
            long cooldownEndTs;
        }

        // =========================
        // 复用 V3 的聚合与正/负反馈挑选（保持一致，减少偏差）
        // =========================
        private Map<String, TagStatistics> aggregateStats(UserNExposures event) {
            Map<String, TagStatistics> statsMap = new HashMap<>();
            long filterTime = Instant.now().getEpochSecond() - (5 * 60);
            for (Tuple4<List<PostViewInfo>, Long, Long, String> tuple : event.firstNExposures) {
                String pornTag = tuple.f3;
                long postId = tuple.f1;
                TagStatistics stats = statsMap.getOrDefault(pornTag, new TagStatistics());
                if (stats.triggerPostId == 0) {
                    stats.triggerPostId = postId;
                }

                boolean hasPositiveBehavior = false;
                boolean hasNegativeBehavior = false;
                boolean hasDislikeBehavior = false;
                PostBehaviorDetail postDetail = null;
                float postTotalStandingTime = 0.0f;
                float postTotalProgressTime = 0.0f;

                for (PostViewInfo info : tuple.f0) {
                    if (info == null) continue;
                    stats.standingTime += info.standingTime;
                    postTotalStandingTime += info.standingTime;
                    postTotalProgressTime += info.progressTime;

                    if (postDetail == null) {
                        postDetail = new PostBehaviorDetail(postId, 0.0f, 0.0f);
                    }

                    if (tuple.f2 > filterTime) {
                        if (info.interaction != null && !info.interaction.isEmpty()) {
                            for (int action : info.interaction) {
                                if (isPositiveAction(action)) {
                                    hasPositiveBehavior = true;
                                    String actionName = getActionName(action);
                                    if (!postDetail.positiveActions.contains(actionName)) {
                                        postDetail.positiveActions.add(actionName);
                                    }
                                } else if (action == 11 || action == 7 || action == 18) {
                                    hasNegativeBehavior = true;
                                    hasDislikeBehavior = true;
                                    String actionName = "dislike";
                                    if (!postDetail.negativeActions.contains(actionName)) {
                                        postDetail.negativeActions.add(actionName);
                                    }
                                }
                            }
                        }
                    }
                }

                if (postDetail != null) {
                    postDetail.standingTime = postTotalStandingTime;
                    postDetail.progressTime = postTotalProgressTime;
                    stats.allPostDetails.add(postDetail);

                    if (postTotalStandingTime >= 5.0f) {
                        stats.longPlayPostDetails.add(postDetail);
                    }

                    if (hasPositiveBehavior) {
                        stats.positiveCount += 1;
                        stats.positivePostDetails.add(postDetail);
                    }

                    // 短播判定（沿用 V3：<3s 记 shortPlayPostCount，用于负反馈触发）
                    if (postTotalStandingTime > 0 && postTotalStandingTime < 3.0f) {
                        stats.shortPlayPostCount += 1;
                        stats.negativeCount += 1;
                        stats.negativePostDetails.add(postDetail);
                        if (!postDetail.negativeActions.contains("short_play")) {
                            postDetail.negativeActions.add("short_play");
                        }
                    }

                    if (hasNegativeBehavior) {
                        stats.negativeCount += 1;
                        stats.negativePostDetails.add(postDetail);
                    }

                    if (hasDislikeBehavior) {
                        stats.dislikeCount += 1;
                    }
                }

                statsMap.put(pornTag, stats);
            }
            return statsMap;
        }

        private Tuple3<String, Long, TagStatistics> pickHighestPositive(Map<String, TagStatistics> statsMap) {
            String bestTag = null;
            long bestPostId = 0;
            int bestLevel = -1;
            TagStatistics bestStats = null;
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || "clean".equals(tag)) continue;

                float maxStandingTime = 0.0f;
                for (PostBehaviorDetail d : s.allPostDetails) {
                    if (d.standingTime > maxStandingTime) maxStandingTime = d.standingTime;
                }
                boolean hasLongPlay = maxStandingTime >= 10.0f;
                boolean positive = hasLongPlay || (s.positiveCount > 0);
                boolean noNegative = s.negativeCount <= 0;
                if (positive && noNegative) {
                    int level = getTagLevel(tag);
                    if (level > bestLevel) {
                        bestLevel = level;
                        bestTag = tag;
                        bestPostId = s.triggerPostId;
                        bestStats = s;
                    }
                }
            }
            return Tuple3.of(bestTag, bestPostId, bestStats);
        }

        private Tuple3<String, Long, TagStatistics> pickLowestNegative(Map<String, TagStatistics> statsMap) {
            String worstTag = null;
            long worstPostId = 0;
            int worstLevel = Integer.MAX_VALUE;
            TagStatistics worstStats = null;
            for (Map.Entry<String, TagStatistics> entry : statsMap.entrySet()) {
                String tag = entry.getKey();
                TagStatistics s = entry.getValue();
                if ("unk".equals(tag) || "clean".equals(tag)) continue;
                if (s.shortPlayPostCount >= 5 || s.dislikeCount >= 1) {
                    int level = getTagLevel(tag);
                    if (level < worstLevel) {
                        worstLevel = level;
                        worstTag = tag;
                        worstPostId = s.triggerPostId;
                        worstStats = s;
                    }
                }
            }
            return Tuple3.of(worstTag, worstPostId, worstStats);
        }

        private boolean isPositiveAction(int action) {
            return action == 1 || action == 3 || action == 5 || action == 6 || action == 9
                    || action == 10 || action == 13 || action == 15 || action == 16;
        }

        private String getActionName(int action) {
            switch (action) {
                case 1: return "点赞";
                case 3: return "评论";
                case 5: return "分享";
                case 6: return "收藏";
                case 9: return "下载";
                case 10: return "购买";
                case 13: return "关注";
                case 15: return "查看主页";
                case 16: return "订阅";
                default: return "正反馈行为" + action;
            }
        }

        private int getTagLevel(String tag) {
            if (tag == null) return 0;
            if (tag.contains("explicit")) return 3;
            if (tag.contains("high")) return 2;
            if (tag.contains("mid")) return 1;
            return 0;
        }
    }

    // =========================
    // 7) RecentNExposures：复用 V3 的“保留最近 N 条记录”策略（为兼容，直接拷贝核心结构）
    // =========================
    public static class UserNExposures implements Serializable {
        public long viewer;
        public List<Tuple4<List<PostViewInfo>, Long, Long, String>> firstNExposures;
    }

    /**
     * 复用 V3 的“最近 N 条曝光/观看”聚合逻辑：
     * - 使用 ListState 存最近记录
     * - 为避免行为过多，这里只拷贝 V3 的结构和关键字段
     *
     * 注意：如果你后续修改 V3 的 RecentNExposures 行为，建议同步到 V5。
     */
    public static class RecentNExposures extends KeyedProcessFunction<Long, PostViewEvent, UserNExposures> {
        private static final int N = 30;     // 与 V3 对齐：保留最近30个postId
        private static final int CAL_NUM = 2; // 与 V3 对齐：至少2个postId后才输出

        private transient ListState<Tuple4<List<PostViewInfo>, Long, Long, String>> recentViewEventState;
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            ListStateDescriptor<Tuple4<List<PostViewInfo>, Long, Long, String>> descriptor =
                    new ListStateDescriptor<>(
                            "recentViewEvent_v5",
                            TypeInformation.of(new TypeHint<Tuple4<List<PostViewInfo>, Long, Long, String>>() {})
                    );
            descriptor.enableTimeToLive(ttlConfig);
            recentViewEventState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(PostViewEvent value, Context ctx, Collector<UserNExposures> out) throws Exception {
            if (value == null) return;
            long uid = value.uid;
            long expoTime = value.createdAt;
            List<PostViewInfo> infos = value.infoList == null ? Collections.emptyList() : value.infoList;
            if (infos.isEmpty()) return;

            // 取出现有记录
            List<Tuple4<List<PostViewInfo>, Long, Long, String>> allRecords = new ArrayList<>();
            for (Tuple4<List<PostViewInfo>, Long, Long, String> record : recentViewEventState.get()) {
                if (record != null) allRecords.add(record);
            }

            // 按 postId 归并；新 postId 需要从 Redis 读取色情标签
            for (PostViewInfo info : infos) {
                if (info == null || info.postId <= 0) continue;

                boolean exist = false;
                for (Tuple4<List<PostViewInfo>, Long, Long, String> record : allRecords) {
                    if (record != null && record.f1 != null && record.f1 == info.postId) {
                        record.f0.add(info);
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    String pornTag = getPostTagFromRedis(info.postId);
                    List<PostViewInfo> one = new ArrayList<>();
                    one.add(info);
                    allRecords.add(Tuple4.of(one, info.postId, expoTime, pornTag));
                }
            }

            // 按曝光时间排序，保留最近 N 个 postId
            allRecords.sort(Comparator.comparing(r -> r.f2));
            if (allRecords.size() > N) {
                allRecords = new ArrayList<>(allRecords.subList(allRecords.size() - N, allRecords.size()));
            }

            recentViewEventState.update(allRecords);

            // 至少 CAL_NUM 个 postId 才输出
            if (allRecords.size() >= CAL_NUM) {
                UserNExposures res = new UserNExposures();
                res.viewer = uid;
                res.firstNExposures = allRecords;
                out.collect(res);
            }
        }

        @Override
        public void close() throws Exception {
            if (redisManager != null) {
                redisManager.shutdown();
            }
            super.close();
        }

        private static final String UNK = "unk";

        private String getPostTagFromRedis(long postId) {
            // 与 V3 对齐：从 rec_post:{postId}:aitag 读取内容标签
            // value 示例："... , restricted#explicit , ..."
            String redisKey = "rec_post:{" + postId + "}:aitag";
            try {
                Tuple2<String, byte[]> tuple = redisManager.getStringCommands().get(redisKey);
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    String raw = new String(tuple.f1, StandardCharsets.UTF_8);
                    if (raw == null || raw.isEmpty()) return UNK;
                    return selectPornTag(raw);
                }
            } catch (Exception e) {
                LOG.warn("[PornStateV5] read post tag fail, postId={}, key={}, err={}", postId, redisKey, e.getMessage());
            }
            return UNK;
        }

        /**
         * 复用 V3 的标签选择规则：优先识别 restricted#xxx
         * - clean -> clean
         * - explicit -> explicit
         * - borderline/mid -> mid
         * - mid-sexy/high -> high
         * - 其他 -> unk
         */
        private String selectPornTag(String rawValue) {
            if (rawValue == null || rawValue.isEmpty()) return UNK;
            String[] tags = rawValue.split(",");
            for (String tag : tags) {
                if (tag == null) continue;
                String trimmed = tag.trim();
                if (trimmed.isEmpty()) continue;
                if (trimmed.contains("restricted#")) {
                    String[] vals = trimmed.split("#");
                    if (vals.length == 2) {
                        String tagType = vals[1];
                        if ("clean".equals(tagType)) return "clean";
                        if ("explicit".equals(tagType)) return "explicit";
                        if ("borderline".equals(tagType) || "mid".equals(tagType)) return "mid";
                        if ("mid-sexy".equals(tagType) || "high".equals(tagType)) return "high";
                        return UNK;
                    }
                    break;
                }
            }
            return UNK;
        }
    }

    /**
     * 将埋点体系的 PostEvent（来自 RecUserPostFeatureLatestJob）转换为 V3/V5 复用的 PostViewEvent/PostViewInfo：
     * - 观看时长相关字段参考 EVENT_VIDEO_PLAY_DURATION 的 standing_time / progress_time / playback_time
     * - 其余正反馈事件来自点赞/评论/分享/关注/点作者主页/付费等事件
     * - 负反馈来自举报/不喜欢，以及 prompt_recommendation_click_no 试探弹窗否定事件
     *
     * 判定正负反馈的底层逻辑仍依赖 V5 里 aggregateStats 中的：
     * - standingTime & progressTime
     * - interaction 列表（正向 action code / dislike action code）
     */
    public static class PostEventToViewEventMapper implements FlatMapFunction<PostEvent, PostViewEvent> {
        @Override
        public void flatMap(PostEvent event, Collector<PostViewEvent> out) {
            if (event == null || event.uid <= 0 || event.postId <= 0) {
                return;
            }

            PostViewEvent viewEvent = new PostViewEvent();
            viewEvent.uid = event.uid;
            // V3 中 createdAt 语义为秒，这里从毫秒时间戳转换为秒，保持量级一致
            viewEvent.createdAt = event.eventTime > 0 ? event.eventTime / 1000L : System.currentTimeMillis() / 1000L;

            PostViewInfo info = new PostViewInfo();
            info.postId = event.postId;
            info.standingTime = (float) event.standingTime;
            info.progressTime = (float) event.progressTime;
            info.viewer = event.uid;
            info.recToken = event.recToken;

            // 根据事件上的布尔标志，构造 interaction 列表
            List<Integer> interactions = new ArrayList<>();
            if (event.isLike) {
                interactions.add(1);  // 点赞
            }
            if (event.isComment) {
                interactions.add(3);  // 评论
            }
            if (event.isShare) {
                interactions.add(5);  // 分享
            }
            if (event.isFavor) {
                interactions.add(6);  // 收藏
            }
            if (event.isPay) {
                interactions.add(10); // 购买/付费
            }
            if (event.isFollow) {
                interactions.add(13); // 关注
            }
            if (event.isProfile) {
                interactions.add(15); // 查看主页/作者资料
            }

            // 负反馈：举报 / 不喜欢 / 试探弹窗否定
            if (event.isReport) {
                interactions.add(11); // 举报
            }
            if (event.isNotInterest) {
                // 通用“不喜欢/不感兴趣”信号
                interactions.add(7);  // 不感兴趣
            }
            // 对于来自 prompt_recommendation_click_no 的事件，会在 PromptNoEventParser 中将 isNotInterest 置为 true，
            // 这里统一走 isNotInterest 的分支并额外打一个 18 作为区分
            if (event.isNotInterest && "prompt_recommendation_click_no".equals(event.eventType)) {
                interactions.add(18); // 试探弹窗否定
            }

            info.interaction = interactions;

            viewEvent.infoList = new ArrayList<>();
            viewEvent.infoList.add(info);

            if (isMonitored(event.uid)) {
                LOG.info("[PornStateV5][Event] uid={}, postId={}, eventType={}, standingTime={}, progressTime={}, playbackTime={}, isLike={}, isComment={}, isShare={}, isFollow={}, isFavor={}, isProfile={}, isPay={}, isReport={}, isNotInterest={}, isCompletePlay={}, interactions={}",
                        event.uid, event.postId, event.eventType,
                        event.standingTime, event.progressTime, event.playbackTime,
                        event.isLike, event.isComment, event.isShare, event.isFollow,
                        event.isFavor, event.isProfile, event.isPay,
                        event.isReport, event.isNotInterest, event.isCompletePlay,
                        interactions);
            }

            out.collect(viewEvent);
        }
    }

    /**
     * 专门解析 prompt_recommendation_click_no 试探弹窗否定事件：
     * - 从 event_post 埋点中抽取 userId/postId
     * - 标记 isNotInterest=true，用于负反馈判定
     *
     * 其余字段按需填充：standingTime/progressTime 默认为 0，由 aggregateStats 统一处理。
     */
    public static class PromptNoEventParser implements FlatMapFunction<String, PostEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String value, Collector<PostEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                return;
            }
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }

                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();
                if (!EVENT_PROMPT_NO.equals(event)) {
                    return;
                }

                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equalsIgnoreCase(eventData)) {
                    return;
                }

                JsonNode data = OBJECT_MAPPER.readTree(eventData);

                // 优先从 user_event_log.uid 获取用户ID（与你提供的示例结构保持一致）
                long userId = userEventLog.path("uid").asLong(0);
                if (userId <= 0) {
                    // 兜底：再尝试从 event_data.userId 中解析（兼容其他埋点格式）
                    JsonNode userIdNode = data.path("userId");
                    if (userIdNode.isTextual()) {
                        try {
                            userId = Long.parseLong(userIdNode.asText("0"));
                        } catch (NumberFormatException ignore) {
                            userId = 0;
                        }
                    } else {
                        userId = userIdNode.asLong(0);
                    }
                }
                long postId = 0;
                if (data.has("post_id")) {
                    String postIdStr = data.path("post_id").asText("");
                    try {
                        postId = Long.parseLong(postIdStr);
                    } catch (NumberFormatException ignore) {
                        postId = 0;
                    }
                }
                if (userId <= 0 || postId <= 0) {
                    return;
                }

                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                }

                PostEvent evt = new PostEvent();
                evt.eventType = EVENT_PROMPT_NO;
                evt.uid = userId;
                evt.postId = postId;
                evt.authorId = 0;
                // 优先从 event_data.rec_token 获取 rec_token，若不存在则置空
                String recToken = data.path("rec_token").asText("");
                evt.recToken = recToken == null ? "" : recToken;
                evt.exposedPos = 0;
                evt.eventTime = eventTimeMillis;
                // 视为一次负反馈信号
                evt.isNotInterest = true;

                out.collect(evt);
            } catch (Exception e) {
                // 静默处理，避免解析异常影响主流程
            }
        }
    }
}


