package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.feature.RecFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 用户实时负反馈标签队列维护任务
 */
public class UserFeatureRealtimeNegativeFeedbacks {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureRealtimeNegativeFeedbacks.class);

    // Redis Key 前缀和后缀
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:latest5negtags";

    // 各队列最大长度
    private static final int MAX_NEGATIVE_TAGS = 5;
    private static final int MAX_NEGATIVE_AUTHORS = 5;
    private static final int MAX_POSITIVE_TAGS = 5;
    private static final int MAX_POSITIVE_AUTHORS = 5;

    // 权重常量
    // 短播放负反馈（< 3秒）
    private static final float NEGATIVE_SHORT_VIEW_TAG_WEIGHT = 0.5f;
    private static final float NEGATIVE_SHORT_VIEW_AUTHOR_WEIGHT = 0.1f;
    // 举报/不感兴趣负反馈
    private static final float NEGATIVE_REPORT_TAG_WEIGHT = 0.1f;
    private static final float NEGATIVE_REPORT_AUTHOR_WEIGHT = 0.1f;
    // 正反馈（≥ 10秒）
    private static final float POSITIVE_TAG_WEIGHT = 1.2f;
    private static final float POSITIVE_AUTHOR_WEIGHT = 1.0f;

    // 区分正负权重的阈值（兼容旧数据）
    // 正反馈标签权重是 1.2，负反馈标签权重是 0.5 或 0.1，所以阈值设为 0.6
    private static final float TAG_POSITIVE_THRESHOLD = 0.6f;
    // 正反馈作者权重是 1.0，负反馈作者权重是 0.1，所以阈值设为 0.5
    private static final float AUTHOR_POSITIVE_THRESHOLD = 0.5f;

    // Redis TTL（6小时，单位：秒）
    private static final int REDIS_TTL = 6 * 3600;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "gosh-negative-feedbacks";

    public static void main(String[] args) throws Exception {
        // 启动调试信息

        try {
            // 第一步：创建 Flink 环境
            StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

            // 第二步：创建 Kafka Source
            java.util.Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);

            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post"
            );

            // 第三步：使用 KafkaSource 创建 DataStream
            DataStreamSource<String> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source"
            );

            // 第四步：预过滤 - 只保留观看事件（event_type=8）
            DataStream<String> filteredStream = kafkaStream
                    .filter(EventFilterUtil.createFastEventTypeFilter(8))
                    .name("Pre-filter View Events");

            // 第五步：解析观看事件
            SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
                    .flatMap(new UserFeatureCommon.ViewEventParser())
                    .name("Parse View Events");

            // 第六步：识别正/负反馈事件
            SingleOutputStreamOperator<FeedbackEvent> feedbackStream = viewStream
                    .flatMap(new FeedbackEventParser())
                    .name("Identify Feedback Events");

            // 第七步：从 Redis 获取视频标签并维护正负反馈队列
            SingleOutputStreamOperator<UserFeedbackQueue> feedbackQueueStream = feedbackStream
                    .keyBy(new KeySelector<FeedbackEvent, Long>() {
                        @Override
                        public Long getKey(FeedbackEvent value) throws Exception {
                            return value.uid;
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(java.time.Duration.ofSeconds(10))) // 10秒窗口，批量处理
                    .process(new FeedbackQueueProcessor())
                    .name("Process Feedback Queues");

            // 第八步：转换为 Protobuf 并写入 Redis
            DataStream<Tuple2<String, byte[]>> dataStream = feedbackQueueStream
                    .map(new MapFunction<UserFeedbackQueue, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(UserFeedbackQueue queue) throws Exception {
                            // 构建 Redis key
                            String redisKey = PREFIX + queue.uid + SUFFIX;

                            // 构建 Protobuf
                            RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();

                            // 添加正反馈标签（使用存储的权重）
                            if (queue.positiveTags != null) {
                                for (TagWithWeight tagWithWeight : queue.positiveTags) {
                                    RecFeature.FeedbackTagV2.Builder tagBuilder = RecFeature.FeedbackTagV2.newBuilder();
                                    tagBuilder.setTag(tagWithWeight.tag);
                                    tagBuilder.setWeight(tagWithWeight.weight);
                                    builder.addFeedbackTags(tagBuilder.build());
                                }
                            }

                            // 添加负反馈标签（使用存储的权重）
                            if (queue.negativeTags != null) {
                                for (TagWithWeight tagWithWeight : queue.negativeTags) {
                                    RecFeature.FeedbackTagV2.Builder tagBuilder = RecFeature.FeedbackTagV2.newBuilder();
                                    tagBuilder.setTag(tagWithWeight.tag);
                                    tagBuilder.setWeight(tagWithWeight.weight);
                                    builder.addFeedbackTags(tagBuilder.build());
                                }
                            }

                            // 添加正反馈作者（使用存储的权重）
                            if (queue.positiveAuthorIds != null) {
                                for (AuthorWithWeight authorWithWeight : queue.positiveAuthorIds) {
                                    if (authorWithWeight.authorId <= 0) {
                                        continue;
                                    }
                                    RecFeature.FeedbackAuthorId.Builder authorBuilder = RecFeature.FeedbackAuthorId.newBuilder();
                                    authorBuilder.setAuthorId(authorWithWeight.authorId);
                                    authorBuilder.setWeight(authorWithWeight.weight);
                                    builder.addFeedbackAuthorIds(authorBuilder.build());
                                }
                            }

                            // 添加负反馈作者（使用存储的权重）
                            if (queue.negativeAuthorIds != null) {
                                for (AuthorWithWeight authorWithWeight : queue.negativeAuthorIds) {
                                    if (authorWithWeight.authorId <= 0) {
                                        continue;
                                    }
                                    RecFeature.FeedbackAuthorId.Builder authorBuilder = RecFeature.FeedbackAuthorId.newBuilder();
                                    authorBuilder.setAuthorId(authorWithWeight.authorId);
                                    authorBuilder.setWeight(authorWithWeight.weight);
                                    builder.addFeedbackAuthorIds(authorBuilder.build());
                                }
                            }

                            byte[] value = builder.build().toByteArray();
                            return new Tuple2<>(redisKey, value);
                        }
                    })
                    .name("Convert to Protobuf");

            // 第九步：创建 Redis Sink
            RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfig.setTtl(REDIS_TTL);

            RedisUtil.addRedisSink(
                    dataStream,
                    redisConfig,
                    true, // 异步写入
                    100   // 批量大小
            );

            // 执行任务
            env.execute("User Feature Realtime Negative Feedbacks Job");

        } catch (Exception e) {
            LOG.error("Flink任务执行失败", e);
            throw e;
        }

    }

    /**
     * 反馈队列处理器：维护正负反馈标签与作者
     */
    private static class FeedbackQueueProcessor extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            FeedbackEvent, UserFeedbackQueue, Long, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        private transient RedisConnectionManager redisConnectionManager;
        private transient RedisConfig redisConfig;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConnectionManager = RedisConnectionManager.getInstance(redisConfig);
            LOG.info("FeedbackQueueProcessor opened with Redis config: {}", redisConfig);
        }

        @Override
        public void close() throws Exception {
            if (redisConnectionManager != null) {
                redisConnectionManager.shutdown();
            }
            super.close();
        }

        @Override
        public void process(Long uid,
                            org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                                    FeedbackEvent, UserFeedbackQueue, Long,
                                    org.apache.flink.streaming.api.windowing.windows.TimeWindow>.Context context,
                            Iterable<FeedbackEvent> elements,
                            Collector<UserFeedbackQueue> out) throws Exception {

            List<FeedbackEvent> events = new ArrayList<>();
            for (FeedbackEvent event : elements) {
                events.add(event);
            }
            if (events.isEmpty()) {
                return;
            }

            ExistingFeedbackState existingState = readExistingFeedbackFromRedis(uid);

            List<CompletableFuture<String>> tagFutures = new ArrayList<>(events.size());
            for (FeedbackEvent event : events) {
                tagFutures.add(getPostTagFromRedis(event.postId));
                handleAuthor(event.authorId, event.feedbackType, existingState);
            }

            for (int i = 0; i < tagFutures.size(); i++) {
                try {
                    String tag = tagFutures.get(i).get();
                    handleTag(tag, events.get(i).feedbackType, existingState);
                } catch (Exception e) {
                    LOG.warn("Failed to get tag for postId {}: {}", events.get(i).postId, e.getMessage());
                }
            }

            if (existingState.hasAny()) {
                UserFeedbackQueue queue = new UserFeedbackQueue();
                queue.uid = uid;
                // 转换带权重的标签
                queue.positiveTags = new ArrayList<>();
                for (Map.Entry<String, Float> entry : existingState.positiveTags.entrySet()) {
                    queue.positiveTags.add(new TagWithWeight(entry.getKey(), entry.getValue()));
                }
                queue.negativeTags = new ArrayList<>();
                for (Map.Entry<String, Float> entry : existingState.negativeTags.entrySet()) {
                    queue.negativeTags.add(new TagWithWeight(entry.getKey(), entry.getValue()));
                }
                // 转换带权重的作者
                queue.positiveAuthorIds = new ArrayList<>();
                for (Map.Entry<Long, Float> entry : existingState.positiveAuthorIds.entrySet()) {
                    queue.positiveAuthorIds.add(new AuthorWithWeight(entry.getKey(), entry.getValue()));
                }
                queue.negativeAuthorIds = new ArrayList<>();
                for (Map.Entry<Long, Float> entry : existingState.negativeAuthorIds.entrySet()) {
                    queue.negativeAuthorIds.add(new AuthorWithWeight(entry.getKey(), entry.getValue()));
                }
                out.collect(queue);
            }
        }

        private void handleAuthor(Long authorId, FeedbackType feedbackType, ExistingFeedbackState state) {
            if (authorId == null || authorId <= 0) {
                return;
            }
            float weight = getAuthorWeight(feedbackType);
            if (feedbackType == FeedbackType.POSITIVE) {
                // 如果已经在负反馈队列中，跳过（冲突处理：负反馈优先）
                if (state.negativeAuthorIds.containsKey(authorId)) {
                    return;
                }
                state.positiveAuthorIds.remove(authorId);
                state.positiveAuthorIds.put(authorId, weight);
                trimQueue(state.positiveAuthorIds, MAX_POSITIVE_AUTHORS);
            } else {
                // 负反馈：从正反馈队列中移除，加入负反馈队列
                state.positiveAuthorIds.remove(authorId);
                state.negativeAuthorIds.remove(authorId);
                state.negativeAuthorIds.put(authorId, weight);
                trimQueue(state.negativeAuthorIds, MAX_NEGATIVE_AUTHORS);
            }
        }

        private void handleTag(String rawTag, FeedbackType feedbackType, ExistingFeedbackState state) {
            if (rawTag == null) {
                return;
            }
            String tag = rawTag.trim();
            if (tag.isEmpty()) {
                return;
            }
            float weight = getTagWeight(feedbackType);
            if (feedbackType == FeedbackType.POSITIVE) {
                // 如果已经在负反馈队列中，跳过（冲突处理：负反馈优先）
                if (state.negativeTags.containsKey(tag)) {
                    return;
                }
                state.positiveTags.remove(tag);
                state.positiveTags.put(tag, weight);
                trimQueue(state.positiveTags, MAX_POSITIVE_TAGS);
            } else {
                // 负反馈：从正反馈队列中移除，加入负反馈队列
                state.positiveTags.remove(tag);
                state.negativeTags.remove(tag);
                state.negativeTags.put(tag, weight);
                trimQueue(state.negativeTags, MAX_NEGATIVE_TAGS);
            }
        }

        private float getTagWeight(FeedbackType feedbackType) {
            switch (feedbackType) {
                case POSITIVE:
                    return POSITIVE_TAG_WEIGHT;
                case NEGATIVE_SHORT:
                    return NEGATIVE_SHORT_VIEW_TAG_WEIGHT;
                case NEGATIVE_REPORT:
                case NEGATIVE_NOT_INTERESTED:
                    return NEGATIVE_REPORT_TAG_WEIGHT;
                default:
                    return NEGATIVE_SHORT_VIEW_TAG_WEIGHT;
            }
        }

        private float getAuthorWeight(FeedbackType feedbackType) {
            switch (feedbackType) {
                case POSITIVE:
                    return POSITIVE_AUTHOR_WEIGHT;
                case NEGATIVE_SHORT:
                    return NEGATIVE_SHORT_VIEW_AUTHOR_WEIGHT;
                case NEGATIVE_REPORT:
                case NEGATIVE_NOT_INTERESTED:
                    return NEGATIVE_REPORT_AUTHOR_WEIGHT;
                default:
                    return NEGATIVE_SHORT_VIEW_AUTHOR_WEIGHT;
            }
        }

        private ExistingFeedbackState readExistingFeedbackFromRedis(long uid) {
            ExistingFeedbackState state = new ExistingFeedbackState();
            try {
                String redisKey = PREFIX + uid + SUFFIX;
                CompletableFuture<org.apache.flink.api.java.tuple.Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(commands -> commands.get(redisKey));
                org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = valueFuture.get();
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    try {
                        List<RecFeature.FeedbackTagV2> feedbackTags = feature.getFeedbackTagsList();
                        for (RecFeature.FeedbackTagV2 feedbackTag : feedbackTags) {
                            String tag = feedbackTag.getTag();
                            if (tag == null || tag.trim().isEmpty()) {
                                continue;
                            }
                            String trimmedTag = tag.trim();
                            float weight = feedbackTag.getWeight();
                            if (isPositiveTagWeight(weight)) {
                                state.positiveTags.put(trimmedTag, weight);
                            } else {
                                state.negativeTags.put(trimmedTag, weight);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get feedback tags list: {}", e.getMessage());
                    }
                    try {
                        List<RecFeature.FeedbackAuthorId> feedbackAuthorIds = feature.getFeedbackAuthorIdsList();
                        for (RecFeature.FeedbackAuthorId feedbackAuthorId : feedbackAuthorIds) {
                            long authorId = feedbackAuthorId.getAuthorId();
                            if (authorId <= 0) {
                                continue;
                            }
                            float weight = feedbackAuthorId.getWeight();
                            if (isPositiveAuthorWeight(weight)) {
                                state.positiveAuthorIds.put(authorId, weight);
                            } else {
                                state.negativeAuthorIds.put(authorId, weight);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get feedback author IDs list: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOG.debug("Failed to read existing feedback from Redis for uid {}: {}", uid, e.getMessage());
            }
            trimQueue(state.positiveTags, MAX_POSITIVE_TAGS);
            trimQueue(state.negativeTags, MAX_NEGATIVE_TAGS);
            trimQueue(state.positiveAuthorIds, MAX_POSITIVE_AUTHORS);
            trimQueue(state.negativeAuthorIds, MAX_NEGATIVE_AUTHORS);
            return state;
        }

        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            String redisKey = "rec_post:{" + postId + "}:aitag";
            return redisConnectionManager.executeStringAsync(
                    commands -> {
                        try {
                            org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = commands.get(redisKey);
                            if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                                String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                                if (!value.isEmpty()) {
                                    return selectPrioritizedTag(value);
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to fetch tag from Redis for postId {}: {}", postId, e.getMessage());
                        }
                        return null;
                    }
            );
        }

        private String selectPrioritizedTag(String rawValue) {
            String[] tags = rawValue.split(",");
            String contentCandidate = null;
            for (String tag : tags) {
                if (tag == null) {
                    continue;
                }
                String trimmed = tag.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.contains("restricted#explicit")) {
                    return trimmed;
                }
                if (contentCandidate == null && trimmed.contains("content")) {
                    contentCandidate = trimmed;
                }
            }
            return contentCandidate;
        }

        private boolean isPositiveTagWeight(float weight) {
            return weight >= TAG_POSITIVE_THRESHOLD;
        }

        private boolean isPositiveAuthorWeight(float weight) {
            return weight >= AUTHOR_POSITIVE_THRESHOLD;
        }

        private <T> void trimQueue(LinkedHashMap<T, Float> queue, int limit) {
            while (queue.size() > limit) {
                Iterator<Map.Entry<T, Float>> iterator = queue.entrySet().iterator();
                if (iterator.hasNext()) {
                    iterator.next();
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }
    /**
     * 反馈类型枚举
     */
    public enum FeedbackType {
        POSITIVE,           // 正反馈（≥ 10秒播放）
        NEGATIVE_SHORT,    // 负反馈：短播放（< 3秒）
        NEGATIVE_REPORT,   // 负反馈：举报（interaction == 7）
        NEGATIVE_NOT_INTERESTED  // 负反馈：不感兴趣（interaction == 11）
    }

    /**
     * 用户反馈事件（可正可负）
     */
    public static class FeedbackEvent {
        public long uid;
        public long postId;
        public long authorId;
        public long timestamp;
        public boolean positive;
        public FeedbackType feedbackType;  // 反馈类型

        @Override
        public String toString() {
            return "FeedbackEvent{uid=" + uid + ", postId=" + postId + ", authorId=" + authorId
                    + ", timestamp=" + timestamp + ", positive=" + positive
                    + ", feedbackType=" + feedbackType + "}";
        }
    }

    /**
     * 带权重的标签信息
     */
    public static class TagWithWeight {
        public String tag;
        public float weight;

        public TagWithWeight(String tag, float weight) {
            this.tag = tag;
            this.weight = weight;
        }
    }

    /**
     * 带权重的作者信息
     */
    public static class AuthorWithWeight {
        public long authorId;
        public float weight;

        public AuthorWithWeight(long authorId, float weight) {
            this.authorId = authorId;
            this.weight = weight;
        }
    }

    /**
     * 用户反馈队列（正负标签、正负作者共存，带权重）
     */
    public static class UserFeedbackQueue {
        public long uid;
        public List<TagWithWeight> positiveTags;
        public List<TagWithWeight> negativeTags;
        public List<AuthorWithWeight> positiveAuthorIds;
        public List<AuthorWithWeight> negativeAuthorIds;

        @Override
        public String toString() {
            return "UserFeedbackQueue{uid=" + uid
                    + ", positiveTags=" + positiveTags
                    + ", negativeTags=" + negativeTags
                    + ", positiveAuthorIds=" + positiveAuthorIds
                    + ", negativeAuthorIds=" + negativeAuthorIds + "}";
        }
    }

    /**
     * 反馈事件解析器：同时识别正负反馈
     */
    private static class FeedbackEventParser implements FlatMapFunction<UserFeatureCommon.PostViewEvent, FeedbackEvent> {
        private static final float POSITIVE_THRESHOLD_SECONDS = 10.0f;
        private static final float NEGATIVE_THRESHOLD_SECONDS = 3.0f;
        private static final int INTERACTION_REPORT = 7;  // 举报
        private static final int INTERACTION_NOT_INTERESTED = 11;  // 不感兴趣

        @Override
        public void flatMap(UserFeatureCommon.PostViewEvent viewEvent, Collector<FeedbackEvent> out) throws Exception {
            if (viewEvent == null || viewEvent.infoList == null) {
                return;
            }

            for (UserFeatureCommon.PostViewInfo info : viewEvent.infoList) {
                if (info.postId <= 0) {
                    continue;
                }

                // 检查交互类型：举报和不感兴趣（优先级最高）
                boolean hasReport = false;
                boolean hasNotInterested = false;
                if (info.interaction != null) {
                    for (Integer interactionType : info.interaction) {
                        if (interactionType == INTERACTION_REPORT) {
                            hasReport = true;
                        } else if (interactionType == INTERACTION_NOT_INTERESTED) {
                            hasNotInterested = true;
                        }
                    }
                }

                // 负反馈：举报（interaction == 7）
                if (hasReport) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_REPORT);
                    out.collect(negative);
                }

                // 负反馈：不感兴趣（interaction == 11）
                if (hasNotInterested) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_NOT_INTERESTED);
                    out.collect(negative);
                }

                // 负反馈：播放 < 3 秒（如果没有举报或不感兴趣）
                if (!hasReport && !hasNotInterested
                        && info.progressTime > 0 && info.progressTime < NEGATIVE_THRESHOLD_SECONDS) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_SHORT);
                    out.collect(negative);
                }

                // 正反馈：播放 ≥ 10 秒
                if (info.progressTime >= POSITIVE_THRESHOLD_SECONDS) {
                    FeedbackEvent positive = buildEvent(viewEvent, info, true, FeedbackType.POSITIVE);
                    out.collect(positive);
                }
            }
        }

        private FeedbackEvent buildEvent(UserFeatureCommon.PostViewEvent viewEvent,
                                         UserFeatureCommon.PostViewInfo info,
                                         boolean positive,
                                         FeedbackType feedbackType) {
            FeedbackEvent event = new FeedbackEvent();
            event.uid = viewEvent.uid;
            event.postId = info.postId;
            event.authorId = info.author;
            event.timestamp = viewEvent.createdAt * 1000;
            event.positive = positive;
            event.feedbackType = feedbackType;
            return event;
        }
    }

    /**
     * 内存中的反馈状态（带权重）
     */
    private static class ExistingFeedbackState {
        // 使用 LinkedHashMap 保持插入顺序，同时存储权重
        private final LinkedHashMap<String, Float> positiveTags = new LinkedHashMap<>();
        private final LinkedHashMap<String, Float> negativeTags = new LinkedHashMap<>();
        private final LinkedHashMap<Long, Float> positiveAuthorIds = new LinkedHashMap<>();
        private final LinkedHashMap<Long, Float> negativeAuthorIds = new LinkedHashMap<>();

        boolean hasAny() {
            return !positiveTags.isEmpty() || !negativeTags.isEmpty()
                    || !positiveAuthorIds.isEmpty() || !negativeAuthorIds.isEmpty();
        }
    }
}
