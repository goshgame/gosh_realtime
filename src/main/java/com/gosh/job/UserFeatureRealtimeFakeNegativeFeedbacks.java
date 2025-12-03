package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.entity.RecFeature;
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
 * 带详细日志的实时正负反馈标签/作者队列维护任务（调试版）
 *
 * 与 {@link UserFeatureRealtimeNegativeFeedbacks} 保持逻辑一致：
 * - 同时维护正负标签、正负作者队列
 * - 支持三种负反馈：短播放、不感兴趣、举报
 * - 使用相同的权重设计
 *
 * 区别：本任务会通过 System.out 打印详细的中间过程，方便线上问题排查。
 */
public class UserFeatureRealtimeFakeNegativeFeedbacks {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureRealtimeFakeNegativeFeedbacks.class);

    // Redis Key 前缀和后缀
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:latest5negtags";

    // 各队列最大长度
    private static final int MAX_NEGATIVE_TAGS = 5;
    private static final int MAX_NEGATIVE_AUTHORS = 5;
    private static final int MAX_POSITIVE_TAGS = 5;
    private static final int MAX_POSITIVE_AUTHORS = 5;

    // 权重常量（与正式任务保持一致）
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

    // Redis TTL（调试任务仍然保持 10 分钟，避免脏数据长时间堆积）
    private static final int REDIS_TTL = 10 * 60;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "gosh-negative-feedbacks-debug";

    // 当无法从 Redis 获取标签时的默认标签集合（包含 content 标签）
    private static final String DEFAULT_POST_TAGS =
            "age#youngadult,gender#male,object#human,content#news,quality#high,emotions#calm,formtype#commentary,"
                    + "language#english,occasion#formal,scenetype#indoor,appearance#bodytype,appearance#beauty_level,"
                    + "imagestyle#cinematic,restricted#clean,functiontype#knowledge";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Debug Flink 任务启动(UserFeatureRealtimeFakeNegativeFeedbacks) ===");
        System.out.println("启动时间: " + new Date());
        System.out.println("参数: " + Arrays.toString(args));

        try {
            // 1. 创建 Flink 环境
            System.out.println("1. 创建 Flink 环境...");
            StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
            System.out.println("Flink 环境创建完成，并行度: " + env.getParallelism());

            // 2. 创建 Kafka Source
            System.out.println("2. 创建 Kafka Source...");
            Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);
            System.out.println("Kafka 配置: " + kafkaProperties);

            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post"
            );
            System.out.println("Kafka Source 创建完成");

            // 3. 使用 KafkaSource 创建 DataStream
            System.out.println("3. 创建 Kafka 数据流...");
            DataStreamSource<String> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source"
            );
            System.out.println("Kafka 数据流创建完成");

            // 4. 预过滤 - 只保留观看事件（event_type=8）
            System.out.println("4. 过滤观看事件...");
            DataStream<String> filteredStream = kafkaStream
                    .filter(EventFilterUtil.createFastEventTypeFilter(8))
                    .name("Pre-filter View Events")
                    .map(value -> {
                        System.out.println("[Kafka] 收到原始消息: " + value);
                        return value;
                    })
                    .name("Debug Kafka Messages");

            // 5. 解析观看事件
            System.out.println("5. 解析观看事件...");
            SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
                    .flatMap(new UserFeatureCommon.ViewEventParser())
                    .name("Parse View Events")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("[ViewEvent] UID: " + event.uid +
                                    ", infoList size: " + (event.infoList != null ? event.infoList.size() : 0));
                        } else {
                            System.out.println("[ViewEvent] 解析结果为 null");
                        }
                        return event;
                    })
                    .name("Debug View Events");

            // 6. 识别正/负反馈事件
            System.out.println("6. 识别正/负反馈事件...");
            SingleOutputStreamOperator<FeedbackEvent> feedbackStream = viewStream
                    .flatMap(new FeedbackEventParser())
                    .name("Identify Feedback Events")
                    .map(event -> {
                        System.out.println("[FeedbackEvent] " + event);
                        return event;
                    })
                    .name("Debug Feedback Events");

            // 7. 维护正负反馈队列（标签 + 作者）
            System.out.println("7. 维护正负反馈队列...");
            SingleOutputStreamOperator<UserFeedbackQueue> feedbackQueueStream = feedbackStream
                    .keyBy(new KeySelector<FeedbackEvent, Long>() {
                        @Override
                        public Long getKey(FeedbackEvent value) {
                            System.out.println("[KeyBy] UID: " + value.uid);
                            return value.uid;
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))) // 10 秒窗口
                    .process(new FeedbackQueueProcessor())
                    .name("Process Feedback Queues")
                    .map(queue -> {
                        System.out.println("[Queue] UID: " + queue.uid
                                + ", positiveTags=" + queue.positiveTags
                                + ", negativeTags=" + queue.negativeTags
                                + ", positiveAuthors=" + queue.positiveAuthorIds
                                + ", negativeAuthors=" + queue.negativeAuthorIds);
                        return queue;
                    })
                    .name("Debug Feedback Queue");

            // 8. 转换为 Protobuf 并写入 Redis
            System.out.println("8. 转换为 Protobuf 并写入 Redis...");
            DataStream<Tuple2<String, byte[]>> dataStream = feedbackQueueStream
                    .map(new MapFunction<UserFeedbackQueue, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(UserFeedbackQueue queue) throws Exception {
                            String redisKey = PREFIX + queue.uid + SUFFIX;
                            System.out.println("[RedisKey] " + redisKey);

                            RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();

                            // 正反馈标签
                            if (queue.positiveTags != null) {
                                for (TagWithWeight tagWithWeight : queue.positiveTags) {
                                    System.out.println("[Write] 正标签: " + tagWithWeight.tag
                                            + ", weight=" + tagWithWeight.weight);
                                    RecFeature.FeedbackTag.Builder tagBuilder = RecFeature.FeedbackTag.newBuilder();
                                    tagBuilder.setTag(tagWithWeight.tag);
                                    tagBuilder.setWeight(tagWithWeight.weight);
                                    builder.addFeedbackTags(tagBuilder.build());
                                }
                            }

                            // 负反馈标签
                            if (queue.negativeTags != null) {
                                for (TagWithWeight tagWithWeight : queue.negativeTags) {
                                    System.out.println("[Write] 负标签: " + tagWithWeight.tag
                                            + ", weight=" + tagWithWeight.weight);
                                    RecFeature.FeedbackTag.Builder tagBuilder = RecFeature.FeedbackTag.newBuilder();
                                    tagBuilder.setTag(tagWithWeight.tag);
                                    tagBuilder.setWeight(tagWithWeight.weight);
                                    builder.addFeedbackTags(tagBuilder.build());
                                }
                            }

                            // 正反馈作者
                            if (queue.positiveAuthorIds != null) {
                                for (AuthorWithWeight authorWithWeight : queue.positiveAuthorIds) {
                                    if (authorWithWeight.authorId <= 0) {
                                        continue;
                                    }
                                    System.out.println("[Write] 正作者: " + authorWithWeight.authorId
                                            + ", weight=" + authorWithWeight.weight);
                                    RecFeature.FeedbackAuthorId.Builder authorBuilder = RecFeature.FeedbackAuthorId.newBuilder();
                                    authorBuilder.setAuthorId(authorWithWeight.authorId);
                                    authorBuilder.setWeight(authorWithWeight.weight);
                                    builder.addFeedbackAuthorIds(authorBuilder.build());
                                }
                            }

                            // 负反馈作者
                            if (queue.negativeAuthorIds != null) {
                                for (AuthorWithWeight authorWithWeight : queue.negativeAuthorIds) {
                                    if (authorWithWeight.authorId <= 0) {
                                        continue;
                                    }
                                    System.out.println("[Write] 负作者: " + authorWithWeight.authorId
                                            + ", weight=" + authorWithWeight.weight);
                                    RecFeature.FeedbackAuthorId.Builder authorBuilder = RecFeature.FeedbackAuthorId.newBuilder();
                                    authorBuilder.setAuthorId(authorWithWeight.authorId);
                                    authorBuilder.setWeight(authorWithWeight.weight);
                                    builder.addFeedbackAuthorIds(authorBuilder.build());
                                }
                            }

                            byte[] value = builder.build().toByteArray();
                            System.out.println("[Protobuf] UID=" + queue.uid + ", bytes=" + value.length);
                            return new Tuple2<>(redisKey, value);
                        }
                    })
                    .name("Convert to Protobuf");

            // 9. 创建 Redis Sink
            System.out.println("9. 创建 Redis Sink...");
            RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfig.setTtl(REDIS_TTL);
            System.out.println("Redis 配置: " + redisConfig);

            RedisUtil.addRedisSink(
                    dataStream,
                    redisConfig,
                    true,
                    100
            );
            System.out.println("Redis Sink 创建完成");

            // 执行任务
            System.out.println("=== 开始执行 Debug Flink 任务 ===");
            env.execute("User Feature Realtime Fake Negative Feedbacks Job");

        } catch (Exception e) {
            System.err.println("!!! Flink Debug 任务执行异常 !!!");
            System.err.println("异常时间: " + new Date());
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }

        System.out.println("=== Flink Debug 任务正常结束 ===");
        System.out.println("结束时间: " + new Date());
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
        public FeedbackType feedbackType;

        @Override
        public String toString() {
            return "FeedbackEvent{uid=" + uid
                    + ", postId=" + postId
                    + ", authorId=" + authorId
                    + ", timestamp=" + timestamp
                    + ", positive=" + positive
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

        @Override
        public String toString() {
            return "TagWithWeight{tag='" + tag + "', weight=" + weight + "}";
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

        @Override
        public String toString() {
            return "AuthorWithWeight{authorId=" + authorId + ", weight=" + weight + "}";
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
     * 反馈事件解析器：同时识别正负反馈（带详细日志）
     */
    private static class FeedbackEventParser implements FlatMapFunction<UserFeatureCommon.PostViewEvent, FeedbackEvent> {
        private static final float POSITIVE_THRESHOLD_SECONDS = 10.0f;
        private static final float NEGATIVE_THRESHOLD_SECONDS = 3.0f;
        private static final int INTERACTION_REPORT = 7;           // 举报
        private static final int INTERACTION_NOT_INTERESTED = 11;  // 不感兴趣

        private transient long counter = 0L;

        @Override
        public void flatMap(UserFeatureCommon.PostViewEvent viewEvent, Collector<FeedbackEvent> out) throws Exception {
            if (viewEvent == null || viewEvent.infoList == null) {
                System.out.println("[FeedbackParser] viewEvent 为空或 infoList 为空");
                return;
            }

            counter++;
            System.out.println("[FeedbackParser] 处理事件序号=" + counter
                    + ", UID=" + viewEvent.uid
                    + ", infoList size=" + viewEvent.infoList.size());

            for (UserFeatureCommon.PostViewInfo info : viewEvent.infoList) {
                if (info.postId <= 0) {
                    continue;
                }

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

                System.out.println("[FeedbackParser] UID=" + viewEvent.uid
                        + ", postId=" + info.postId
                        + ", progressTime=" + info.progressTime
                        + ", hasReport=" + hasReport
                        + ", hasNotInterested=" + hasNotInterested);

                // 举报
                if (hasReport) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_REPORT);
                    System.out.println("[FeedbackParser] 生成举报负反馈事件: " + negative);
                    out.collect(negative);
                }

                // 不感兴趣
                if (hasNotInterested) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_NOT_INTERESTED);
                    System.out.println("[FeedbackParser] 生成不感兴趣负反馈事件: " + negative);
                    out.collect(negative);
                }

                // 短播放负反馈（如果没有举报/不感兴趣）
                if (!hasReport && !hasNotInterested
                        && info.progressTime > 0 && info.progressTime < NEGATIVE_THRESHOLD_SECONDS) {
                    FeedbackEvent negative = buildEvent(viewEvent, info, false, FeedbackType.NEGATIVE_SHORT);
                    System.out.println("[FeedbackParser] 生成短播放负反馈事件: " + negative);
                    out.collect(negative);
                }

                // 正反馈：播放 ≥ 10 秒
                if (info.progressTime >= POSITIVE_THRESHOLD_SECONDS) {
                    FeedbackEvent positive = buildEvent(viewEvent, info, true, FeedbackType.POSITIVE);
                    System.out.println("[FeedbackParser] 生成正反馈事件: " + positive);
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
        private final LinkedHashMap<String, Float> positiveTags = new LinkedHashMap<>();
        private final LinkedHashMap<String, Float> negativeTags = new LinkedHashMap<>();
        private final LinkedHashMap<Long, Float> positiveAuthorIds = new LinkedHashMap<>();
        private final LinkedHashMap<Long, Float> negativeAuthorIds = new LinkedHashMap<>();

        boolean hasAny() {
            return !positiveTags.isEmpty() || !negativeTags.isEmpty()
                    || !positiveAuthorIds.isEmpty() || !negativeAuthorIds.isEmpty();
        }
    }

    /**
     * 带详细日志的反馈队列处理器
     */
    private static class FeedbackQueueProcessor extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            FeedbackEvent, UserFeedbackQueue, Long, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        private transient RedisConnectionManager redisConnectionManager;
        private transient RedisConfig redisConfig;
        private transient long windowCount = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("[QueueProcessor] open() 初始化 Redis 配置...");
            redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConnectionManager = RedisConnectionManager.getInstance(redisConfig);
            LOG.info("FeedbackQueueProcessor opened with Redis config: {}", redisConfig);
            System.out.println("[QueueProcessor] Redis 配置: " + redisConfig);
        }

        @Override
        public void close() throws Exception {
            System.out.println("[QueueProcessor] close()，共处理窗口数: " + windowCount);
            if (redisConnectionManager != null) {
                redisConnectionManager.shutdown();
                System.out.println("[QueueProcessor] Redis 连接已关闭");
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

            windowCount++;
            System.out.println("=== [QueueProcessor] 处理窗口 #" + windowCount
                    + " UID=" + uid
                    + " window=" + context.window().getStart() + "-" + context.window().getEnd() + " ===");

            List<FeedbackEvent> events = new ArrayList<>();
            for (FeedbackEvent event : elements) {
                events.add(event);
            }
            System.out.println("[QueueProcessor] 窗口内反馈事件数量: " + events.size());
            if (events.isEmpty()) {
                System.out.println("[QueueProcessor] 无事件，跳过窗口");
                return;
            }

            // 读取 Redis 中已有的反馈状态
            ExistingFeedbackState existingState = readExistingFeedbackFromRedis(uid);
            System.out.println("[QueueProcessor] 初始状态 - "
                    + "posTags=" + existingState.positiveTags
                    + ", negTags=" + existingState.negativeTags
                    + ", posAuthors=" + existingState.positiveAuthorIds
                    + ", negAuthors=" + existingState.negativeAuthorIds);

            // 先处理作者（本身不依赖标签）
            List<CompletableFuture<String>> tagFutures = new ArrayList<>(events.size());
            for (FeedbackEvent event : events) {
                System.out.println("[QueueProcessor] 处理事件: " + event);
                tagFutures.add(getPostTagFromRedis(event.postId));
                handleAuthor(event.authorId, event.feedbackType, existingState);
            }

            // 然后处理标签（需要从 Redis / 默认集合获取）
            for (int i = 0; i < tagFutures.size(); i++) {
                FeedbackEvent event = events.get(i);
                try {
                    String tag = tagFutures.get(i).get();
                    System.out.println("[QueueProcessor] 获取到标签 postId=" + event.postId + " tag=" + tag);
                    handleTag(tag, event.feedbackType, existingState);
                } catch (Exception e) {
                    System.err.println("[QueueProcessor] 获取标签失败 postId=" + event.postId + " err=" + e.getMessage());
                    LOG.warn("Failed to get tag for postId {}: {}", event.postId, e.getMessage());
                }
            }

            System.out.println("[QueueProcessor] 更新后状态 - "
                    + "posTags=" + existingState.positiveTags
                    + ", negTags=" + existingState.negativeTags
                    + ", posAuthors=" + existingState.positiveAuthorIds
                    + ", negAuthors=" + existingState.negativeAuthorIds);

            if (existingState.hasAny()) {
                UserFeedbackQueue queue = new UserFeedbackQueue();
                queue.uid = uid;
                queue.positiveTags = new ArrayList<>();
                for (Map.Entry<String, Float> entry : existingState.positiveTags.entrySet()) {
                    queue.positiveTags.add(new TagWithWeight(entry.getKey(), entry.getValue()));
                }
                queue.negativeTags = new ArrayList<>();
                for (Map.Entry<String, Float> entry : existingState.negativeTags.entrySet()) {
                    queue.negativeTags.add(new TagWithWeight(entry.getKey(), entry.getValue()));
                }
                queue.positiveAuthorIds = new ArrayList<>();
                for (Map.Entry<Long, Float> entry : existingState.positiveAuthorIds.entrySet()) {
                    queue.positiveAuthorIds.add(new AuthorWithWeight(entry.getKey(), entry.getValue()));
                }
                queue.negativeAuthorIds = new ArrayList<>();
                for (Map.Entry<Long, Float> entry : existingState.negativeAuthorIds.entrySet()) {
                    queue.negativeAuthorIds.add(new AuthorWithWeight(entry.getKey(), entry.getValue()));
                }

                System.out.println("[QueueProcessor] 输出队列: " + queue);
                out.collect(queue);
            } else {
                System.out.println("[QueueProcessor] 状态为空，不输出队列");
            }
        }

        private void handleAuthor(Long authorId, FeedbackType feedbackType, ExistingFeedbackState state) {
            if (authorId == null || authorId <= 0) {
                return;
            }
            float weight = getAuthorWeight(feedbackType);
            if (feedbackType == FeedbackType.POSITIVE) {
                if (state.negativeAuthorIds.containsKey(authorId)) {
                    System.out.println("[QueueProcessor] 正作者命中负队列，跳过 authorId=" + authorId);
                    return;
                }
                state.positiveAuthorIds.remove(authorId);
                state.positiveAuthorIds.put(authorId, weight);
                trimQueue(state.positiveAuthorIds, MAX_POSITIVE_AUTHORS);
                System.out.println("[QueueProcessor] 加入正作者队列 authorId=" + authorId + ", weight=" + weight);
            } else {
                state.positiveAuthorIds.remove(authorId);
                state.negativeAuthorIds.remove(authorId);
                state.negativeAuthorIds.put(authorId, weight);
                trimQueue(state.negativeAuthorIds, MAX_NEGATIVE_AUTHORS);
                System.out.println("[QueueProcessor] 加入负作者队列 authorId=" + authorId + ", weight=" + weight
                        + ", type=" + feedbackType);
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
                if (state.negativeTags.containsKey(tag)) {
                    System.out.println("[QueueProcessor] 正标签命中负队列，跳过 tag=" + tag);
                    return;
                }
                state.positiveTags.remove(tag);
                state.positiveTags.put(tag, weight);
                trimQueue(state.positiveTags, MAX_POSITIVE_TAGS);
                System.out.println("[QueueProcessor] 加入正标签队列 tag=" + tag + ", weight=" + weight);
            } else {
                state.positiveTags.remove(tag);
                state.negativeTags.remove(tag);
                state.negativeTags.put(tag, weight);
                trimQueue(state.negativeTags, MAX_NEGATIVE_TAGS);
                System.out.println("[QueueProcessor] 加入负标签队列 tag=" + tag + ", weight=" + weight
                        + ", type=" + feedbackType);
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
                System.out.println("[QueueProcessor] 从 Redis 读取历史反馈 key=" + redisKey);
                CompletableFuture<Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(commands -> commands.get(redisKey));
                Tuple2<String, byte[]> tuple = valueFuture.get();
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    System.out.println("[QueueProcessor] 读取到历史字节长度: " + tuple.f1.length);
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    try {
                        List<RecFeature.FeedbackTag> feedbackTags = feature.getFeedbackTagsList();
                        System.out.println("[QueueProcessor] 历史标签数量: " + feedbackTags.size());
                        for (RecFeature.FeedbackTag feedbackTag : feedbackTags) {
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
                        System.err.println("[QueueProcessor] 解析历史标签失败: " + e.getMessage());
                        LOG.warn("Failed to get feedback tags list: {}", e.getMessage());
                    }
                    try {
                        List<RecFeature.FeedbackAuthorId> feedbackAuthorIds = feature.getFeedbackAuthorIdsList();
                        System.out.println("[QueueProcessor] 历史作者数量: " + feedbackAuthorIds.size());
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
                        System.err.println("[QueueProcessor] 解析历史作者失败: " + e.getMessage());
                        LOG.warn("Failed to get feedback author IDs list: {}", e.getMessage());
                    }
                } else {
                    System.out.println("[QueueProcessor] Redis 中无历史反馈数据");
                }
            } catch (Exception e) {
                System.err.println("[QueueProcessor] 从 Redis 读取历史反馈异常: " + e.getMessage());
                LOG.debug("Failed to read existing feedback from Redis for uid {}: {}", uid, e.getMessage());
            }
            trimQueue(state.positiveTags, MAX_POSITIVE_TAGS);
            trimQueue(state.negativeTags, MAX_NEGATIVE_TAGS);
            trimQueue(state.positiveAuthorIds, MAX_POSITIVE_AUTHORS);
            trimQueue(state.negativeAuthorIds, MAX_NEGATIVE_AUTHORS);
            return state;
        }

        /**
         * Debug 版本：不从 Redis 读取视频标签，而是使用本地 DEFAULT_POST_TAGS，
         * 只为观察队列行为，不依赖真实线上标签。
         */
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            System.out.println("[QueueProcessor] 获取视频标签（调试版，使用默认标签） postId=" + postId);
            return CompletableFuture.completedFuture(selectPrioritizedTag(DEFAULT_POST_TAGS));
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
}