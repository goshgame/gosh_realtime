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
 * 用户实时时间计数加权反馈标签队列维护任务
 *
 * 核心特性：
 * 1. 合并正负反馈队列，最多保持10个标签/作者
 * 2. 基于时间衰减和计数累加的权重计算
 * 3. 负反馈给负分，正反馈给正分，最终权重在0-2之间
 * 4. 显性负反馈（举报/不喜欢）惩罚更大，隐性负反馈（短播）惩罚更小
 * 5. 负反馈时间衰减弱，正反馈时间衰减强
 */
public class UserFeatureRealtimeTimeCountWeightedFeedbacks {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureRealtimeTimeCountWeightedFeedbacks.class);

    // Redis Key 前缀和后缀
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:latest10overalltags";

    // 合并队列最大长度（正负反馈合并后）
    private static final int MAX_OVERALL_TAGS = 10;
    private static final int MAX_OVERALL_AUTHORS = 10;

    // 基础权重（单次反馈的基础分数）
    // 正反馈：奖励较小
    private static final float BASE_POSITIVE_WEIGHT = 0.3f;
    // 短播负反馈：惩罚较小（隐性负反馈）
    private static final float BASE_NEGATIVE_SHORT_WEIGHT = -0.2f;
    // 举报/不喜欢负反馈：惩罚较大（显性负反馈）
    private static final float BASE_NEGATIVE_REPORT_WEIGHT = -0.5f;

    // 时间衰减参数（小时）
    // 正反馈衰减半衰期：6小时（衰减快）
    private static final float POSITIVE_HALF_LIFE_HOURS = 6.0f;
    // 负反馈衰减半衰期：24小时（衰减慢）
    private static final float NEGATIVE_HALF_LIFE_HOURS = 24.0f;

    // 权重范围限制
    private static final float MIN_WEIGHT = 0.0f;
    private static final float MAX_WEIGHT = 2.0f;

    // Redis TTL（12小时，单位：秒）
    private static final int REDIS_TTL = 12 * 3600;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "gosh-timestamp-count-weighted-feedbacks";

    public static void main(String[] args) throws Exception {
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

            // 第七步：维护时间计数加权反馈队列
            SingleOutputStreamOperator<OverallFeedbackQueue> feedbackQueueStream = feedbackStream
                    .keyBy(new KeySelector<FeedbackEvent, Long>() {
                        @Override
                        public Long getKey(FeedbackEvent value) throws Exception {
                            return value.uid;
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(java.time.Duration.ofSeconds(10)))
                    .process(new TimeCountWeightedQueueProcessor())
                    .name("Process Time-Count Weighted Feedback Queues");

            // 第八步：转换为 Protobuf 并写入 Redis
            DataStream<Tuple2<String, byte[]>> dataStream = feedbackQueueStream
                    .map(new MapFunction<OverallFeedbackQueue, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(OverallFeedbackQueue queue) throws Exception {
                            String redisKey = PREFIX + queue.uid + SUFFIX;

                            RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();

                            // 添加标签（合并后的队列，权重在0-2之间）
                            if (queue.tags != null) {
                                for (TagWithScore tagWithScore : queue.tags) {
                                    RecFeature.FeedbackTag.Builder tagBuilder = RecFeature.FeedbackTag.newBuilder();
                                    tagBuilder.setTag(tagWithScore.tag);
                                    tagBuilder.setWeight(tagWithScore.score);
                                    builder.addFeedbackTags(tagBuilder.build());
                                }
                            }

                            // 添加作者（合并后的队列，权重在0-2之间）
                            if (queue.authorIds != null) {
                                for (AuthorWithScore authorWithScore : queue.authorIds) {
                                    if (authorWithScore.authorId <= 0) {
                                        continue;
                                    }
                                    RecFeature.FeedbackAuthorId.Builder authorBuilder = RecFeature.FeedbackAuthorId.newBuilder();
                                    authorBuilder.setAuthorId(authorWithScore.authorId);
                                    authorBuilder.setWeight(authorWithScore.score);
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
            env.execute("User Feature Realtime Time-Count Weighted Feedbacks Job");

        } catch (Exception e) {
            LOG.error("Flink任务执行失败", e);
            throw e;
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
    public static class FeedbackEvent implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long uid;
        public long postId;
        public long authorId;
        public long timestamp;  // 毫秒时间戳
        public boolean positive;
        public FeedbackType feedbackType;

        public FeedbackEvent() {
            // Flink POJO 要求无参构造函数
        }

        @Override
        public String toString() {
            return "FeedbackEvent{uid=" + uid + ", postId=" + postId + ", authorId=" + authorId
                    + ", timestamp=" + timestamp + ", positive=" + positive
                    + ", feedbackType=" + feedbackType + "}";
        }
    }

    /**
     * 带得分和时间的标签信息（用于内部计算）
     */
    private static class TagWithScoreAndTime {
        String tag;
        float positiveScore;  // 正反馈累计得分（>= 0）
        float negativeScore;  // 负反馈累计得分（>= 0，存储绝对值）
        long lastUpdateTime;  // 最后更新时间（毫秒）
        int positiveCount;  // 正反馈次数
        int negativeCount;  // 负反馈次数

        TagWithScoreAndTime(String tag, float positiveScore, float negativeScore,
                            long lastUpdateTime, int positiveCount, int negativeCount) {
            this.tag = tag;
            this.positiveScore = positiveScore;
            this.negativeScore = negativeScore;
            this.lastUpdateTime = lastUpdateTime;
            this.positiveCount = positiveCount;
            this.negativeCount = negativeCount;
        }
    }

    /**
     * 带得分的标签信息（用于输出）
     */
    public static class TagWithScore {
        public String tag;
        public float score;  // 最终权重（0-2之间）

        public TagWithScore(String tag, float score) {
            this.tag = tag;
            this.score = score;
        }
    }

    /**
     * 带得分和时间的作者信息（用于内部计算）
     */
    private static class AuthorWithScoreAndTime {
        long authorId;
        float positiveScore;  // 正反馈累计得分（>= 0）
        float negativeScore;  // 负反馈累计得分（>= 0，存储绝对值）
        long lastUpdateTime;  // 最后更新时间（毫秒）
        int positiveCount;  // 正反馈次数
        int negativeCount;  // 负反馈次数

        AuthorWithScoreAndTime(long authorId, float positiveScore, float negativeScore,
                               long lastUpdateTime, int positiveCount, int negativeCount) {
            this.authorId = authorId;
            this.positiveScore = positiveScore;
            this.negativeScore = negativeScore;
            this.lastUpdateTime = lastUpdateTime;
            this.positiveCount = positiveCount;
            this.negativeCount = negativeCount;
        }
    }

    /**
     * 带得分的作者信息（用于输出）
     */
    public static class AuthorWithScore {
        public long authorId;
        public float score;  // 最终权重（0-2之间）

        public AuthorWithScore(long authorId, float score) {
            this.authorId = authorId;
            this.score = score;
        }
    }

    /**
     * 合并后的反馈队列（正负反馈合并，最多10个）
     */
    public static class OverallFeedbackQueue {
        public long uid;
        public List<TagWithScore> tags;
        public List<AuthorWithScore> authorIds;

        @Override
        public String toString() {
            return "OverallFeedbackQueue{uid=" + uid
                    + ", tags=" + tags
                    + ", authorIds=" + authorIds + "}";
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
     * 时间计数加权反馈队列处理器
     */
    private static class TimeCountWeightedQueueProcessor extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            FeedbackEvent, OverallFeedbackQueue, Long, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        private transient RedisConnectionManager redisConnectionManager;
        private transient RedisConfig redisConfig;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConnectionManager = RedisConnectionManager.getInstance(redisConfig);
            LOG.info("TimeCountWeightedQueueProcessor opened with Redis config: {}", redisConfig);
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
                                    FeedbackEvent, OverallFeedbackQueue, Long,
                                    org.apache.flink.streaming.api.windowing.windows.TimeWindow>.Context context,
                            Iterable<FeedbackEvent> elements,
                            Collector<OverallFeedbackQueue> out) throws Exception {

            List<FeedbackEvent> events = new ArrayList<>();
            for (FeedbackEvent event : elements) {
                events.add(event);
            }
            if (events.isEmpty()) {
                return;
            }

            long currentTime = System.currentTimeMillis();

            // 读取 Redis 中已有的反馈状态
            Map<String, TagWithScoreAndTime> existingTags = readExistingFeedbackFromRedis(uid, currentTime);
            Map<Long, AuthorWithScoreAndTime> existingAuthors = readExistingAuthorsFromRedis(uid, currentTime);

            // 处理新事件
            List<CompletableFuture<String>> tagFutures = new ArrayList<>(events.size());
            for (FeedbackEvent event : events) {
                tagFutures.add(getPostTagFromRedis(event.postId));
                handleAuthor(event, existingAuthors, currentTime);
            }

            for (int i = 0; i < tagFutures.size(); i++) {
                try {
                    String tag = tagFutures.get(i).get();
                    if (tag != null && !tag.trim().isEmpty()) {
                        handleTag(tag.trim(), events.get(i), existingTags, currentTime);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to get tag for postId {}: {}", events.get(i).postId, e.getMessage());
                }
            }

            // 应用时间衰减并转换为输出格式
            if (!existingTags.isEmpty() || !existingAuthors.isEmpty()) {
                OverallFeedbackQueue queue = new OverallFeedbackQueue();
                queue.uid = uid;

                // 转换标签：应用时间衰减，限制在0-2之间
                queue.tags = new ArrayList<>();
                for (TagWithScoreAndTime tagData : existingTags.values()) {
                    float finalScore = applyTimeDecayAndNormalize(tagData, currentTime);
                    // 只保留有意义的标签（得分不在1.0附近，即有明显正负倾向）
                    if (Math.abs(finalScore - 1.0f) > 0.01f) {
                        queue.tags.add(new TagWithScore(tagData.tag, finalScore));
                    }
                }
                // 按得分降序排序
                queue.tags.sort((a, b) -> Float.compare(b.score, a.score));
                // 限制最多10个
                if (queue.tags.size() > MAX_OVERALL_TAGS) {
                    queue.tags = queue.tags.subList(0, MAX_OVERALL_TAGS);
                }

                // 转换作者：应用时间衰减，限制在0-2之间
                queue.authorIds = new ArrayList<>();
                for (AuthorWithScoreAndTime authorData : existingAuthors.values()) {
                    float finalScore = applyTimeDecayAndNormalize(authorData, currentTime);
                    // 只保留有意义的作者（得分不在1.0附近，即有明显正负倾向）
                    if (Math.abs(finalScore - 1.0f) > 0.01f) {
                        queue.authorIds.add(new AuthorWithScore(authorData.authorId, finalScore));
                    }
                }
                // 按得分降序排序
                queue.authorIds.sort((a, b) -> Float.compare(b.score, a.score));
                // 限制最多10个
                if (queue.authorIds.size() > MAX_OVERALL_AUTHORS) {
                    queue.authorIds = queue.authorIds.subList(0, MAX_OVERALL_AUTHORS);
                }

                out.collect(queue);
            }
        }

        /**
         * 处理标签反馈
         */
        private void handleTag(String tag, FeedbackEvent event,
                               Map<String, TagWithScoreAndTime> existingTags, long currentTime) {
            float baseWeight = Math.abs(getBaseWeight(event.feedbackType));  // 取绝对值

            // 如果标签已存在，累加得分
            if (existingTags.containsKey(tag)) {
                TagWithScoreAndTime existing = existingTags.get(tag);
                // 先应用时间衰减到现有得分
                float decayedPositive = applyTimeDecay(existing.positiveScore, existing.lastUpdateTime,
                        currentTime, true);
                float decayedNegative = applyTimeDecay(existing.negativeScore, existing.lastUpdateTime,
                        currentTime, false);
                // 根据反馈类型累加
                if (event.positive) {
                    existing.positiveScore = decayedPositive + baseWeight;
                    existing.positiveCount++;
                } else {
                    existing.negativeScore = decayedNegative + baseWeight;
                    existing.negativeCount++;
                }
                existing.lastUpdateTime = currentTime;
            } else {
                // 新标签：如果队列已满，移除更新时间最久远的元素
                if (existingTags.size() >= MAX_OVERALL_TAGS) {
                    removeOldestTag(existingTags);
                }
                // 添加新标签
                if (event.positive) {
                    existingTags.put(tag, new TagWithScoreAndTime(tag, baseWeight, 0.0f, currentTime, 1, 0));
                } else {
                    existingTags.put(tag, new TagWithScoreAndTime(tag, 0.0f, baseWeight, currentTime, 0, 1));
                }
            }
        }

        /**
         * 处理作者反馈
         */
        private void handleAuthor(FeedbackEvent event,
                                  Map<Long, AuthorWithScoreAndTime> existingAuthors, long currentTime) {
            if (event.authorId <= 0) {
                return;
            }

            float baseWeight = Math.abs(getBaseWeight(event.feedbackType));  // 取绝对值

            // 如果作者已存在，累加得分
            if (existingAuthors.containsKey(event.authorId)) {
                AuthorWithScoreAndTime existing = existingAuthors.get(event.authorId);
                // 先应用时间衰减到现有得分
                float decayedPositive = applyTimeDecay(existing.positiveScore, existing.lastUpdateTime,
                        currentTime, true);
                float decayedNegative = applyTimeDecay(existing.negativeScore, existing.lastUpdateTime,
                        currentTime, false);
                // 根据反馈类型累加
                if (event.positive) {
                    existing.positiveScore = decayedPositive + baseWeight;
                    existing.positiveCount++;
                } else {
                    existing.negativeScore = decayedNegative + baseWeight;
                    existing.negativeCount++;
                }
                existing.lastUpdateTime = currentTime;
            } else {
                // 新作者：如果队列已满，移除更新时间最久远的元素
                if (existingAuthors.size() >= MAX_OVERALL_AUTHORS) {
                    removeOldestAuthor(existingAuthors);
                }
                // 添加新作者
                if (event.positive) {
                    existingAuthors.put(event.authorId,
                            new AuthorWithScoreAndTime(event.authorId, baseWeight, 0.0f, currentTime, 1, 0));
                } else {
                    existingAuthors.put(event.authorId,
                            new AuthorWithScoreAndTime(event.authorId, 0.0f, baseWeight, currentTime, 0, 1));
                }
            }
        }

        /**
         * 获取基础权重（单次反馈的基础分数）
         */
        private float getBaseWeight(FeedbackType feedbackType) {
            switch (feedbackType) {
                case POSITIVE:
                    return BASE_POSITIVE_WEIGHT;
                case NEGATIVE_SHORT:
                    return BASE_NEGATIVE_SHORT_WEIGHT;
                case NEGATIVE_REPORT:
                case NEGATIVE_NOT_INTERESTED:
                    return BASE_NEGATIVE_REPORT_WEIGHT;
                default:
                    return BASE_NEGATIVE_SHORT_WEIGHT;
            }
        }

        /**
         * 应用时间衰减（指数衰减）
         * @param score 当前得分
         * @param lastUpdateTime 最后更新时间（毫秒）
         * @param currentTime 当前时间（毫秒）
         * @param isPositive 是否为正反馈
         * @return 衰减后的得分
         */
        private float applyTimeDecay(float score, long lastUpdateTime, long currentTime, boolean isPositive) {
            if (score == 0) {
                return 0;
            }

            float halfLifeHours = isPositive ? POSITIVE_HALF_LIFE_HOURS : NEGATIVE_HALF_LIFE_HOURS;
            float hoursElapsed = (currentTime - lastUpdateTime) / (1000.0f * 3600.0f);

            // 指数衰减：score * (0.5 ^ (hoursElapsed / halfLifeHours))
            float decayFactor = (float) Math.pow(0.5, hoursElapsed / halfLifeHours);
            return score * decayFactor;
        }

        /**
         * 应用时间衰减并归一化到0-2范围
         * 负反馈得分映射到0-1，正反馈得分映射到1-2
         */
        private float applyTimeDecayAndNormalize(TagWithScoreAndTime tagData, long currentTime) {
            // 分别应用时间衰减
            float decayedPositive = applyTimeDecay(tagData.positiveScore, tagData.lastUpdateTime,
                    currentTime, true);
            float decayedNegative = applyTimeDecay(tagData.negativeScore, tagData.lastUpdateTime,
                    currentTime, false);

            // 计算净得分
            float netScore = decayedPositive - decayedNegative;

            // 根据净得分的正负来映射
            if (netScore > 0) {
                // 正反馈占优：映射到1-2范围
                // 使用sigmoid将正得分映射到0-1，然后+1
                float normalized = 1.0f / (1.0f + (float) Math.exp(-netScore));
                return 1.0f + normalized;  // 范围：1-2
            } else if (netScore < 0) {
                // 负反馈占优：映射到0-1范围
                // 使用sigmoid将负得分（取绝对值）映射到0-1
                float normalized = 1.0f / (1.0f + (float) Math.exp(netScore));
                return normalized;  // 范围：0-1
            } else {
                // 平衡：返回1.0
                return 1.0f;
            }
        }

        /**
         * 应用时间衰减并归一化到0-2范围（作者版本）
         * 负反馈得分映射到0-1，正反馈得分映射到1-2
         */
        private float applyTimeDecayAndNormalize(AuthorWithScoreAndTime authorData, long currentTime) {
            // 分别应用时间衰减
            float decayedPositive = applyTimeDecay(authorData.positiveScore, authorData.lastUpdateTime,
                    currentTime, true);
            float decayedNegative = applyTimeDecay(authorData.negativeScore, authorData.lastUpdateTime,
                    currentTime, false);

            // 计算净得分
            float netScore = decayedPositive - decayedNegative;

            // 根据净得分的正负来映射
            if (netScore > 0) {
                // 正反馈占优：映射到1-2范围
                float normalized = 1.0f / (1.0f + (float) Math.exp(-netScore));
                return 1.0f + normalized;  // 范围：1-2
            } else if (netScore < 0) {
                // 负反馈占优：映射到0-1范围
                float normalized = 1.0f / (1.0f + (float) Math.exp(netScore));
                return normalized;  // 范围：0-1
            } else {
                // 平衡：返回1.0
                return 1.0f;
            }
        }

        /**
         * 移除更新时间最久远的标签
         */
        private void removeOldestTag(Map<String, TagWithScoreAndTime> tags) {
            String oldestTag = null;
            long oldestTime = Long.MAX_VALUE;

            for (Map.Entry<String, TagWithScoreAndTime> entry : tags.entrySet()) {
                if (entry.getValue().lastUpdateTime < oldestTime) {
                    oldestTime = entry.getValue().lastUpdateTime;
                    oldestTag = entry.getKey();
                }
            }

            if (oldestTag != null) {
                tags.remove(oldestTag);
            }
        }

        /**
         * 移除更新时间最久远的作者
         */
        private void removeOldestAuthor(Map<Long, AuthorWithScoreAndTime> authors) {
            Long oldestAuthor = null;
            long oldestTime = Long.MAX_VALUE;

            for (Map.Entry<Long, AuthorWithScoreAndTime> entry : authors.entrySet()) {
                if (entry.getValue().lastUpdateTime < oldestTime) {
                    oldestTime = entry.getValue().lastUpdateTime;
                    oldestAuthor = entry.getKey();
                }
            }

            if (oldestAuthor != null) {
                authors.remove(oldestAuthor);
            }
        }

        /**
         * 从 Redis 读取现有反馈（标签和作者）
         * 注意：由于我们只存储归一化后的权重，需要反向计算原始得分
         */
        private Map<String, TagWithScoreAndTime> readExistingFeedbackFromRedis(long uid, long currentTime) {
            Map<String, TagWithScoreAndTime> tags = new HashMap<>();
            try {
                String redisKey = PREFIX + uid + SUFFIX;
                CompletableFuture<org.apache.flink.api.java.tuple.Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(commands -> commands.get(redisKey));
                org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = valueFuture.get();
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    try {
                        List<RecFeature.FeedbackTag> feedbackTags = feature.getFeedbackTagsList();
                        for (RecFeature.FeedbackTag feedbackTag : feedbackTags) {
                            String tag = feedbackTag.getTag();
                            if (tag == null || tag.trim().isEmpty()) {
                                continue;
                            }
                            String trimmedTag = tag.trim();
                            float normalizedWeight = feedbackTag.getWeight();

                            // 从归一化权重反推净得分
                            float netScore = 0.0f;
                            if (normalizedWeight > 1.0f && normalizedWeight <= 2.0f) {
                                // 正反馈占优：权重在1-2之间
                                float sigmoidValue = normalizedWeight - 1.0f;
                                if (sigmoidValue > 0.01f && sigmoidValue < 0.99f) {
                                    netScore = -(float) Math.log(1.0f / sigmoidValue - 1.0f);
                                } else {
                                    netScore = 3.0f;  // 接近上限
                                }
                            } else if (normalizedWeight >= 0.0f && normalizedWeight < 1.0f) {
                                // 负反馈占优：权重在0-1之间
                                if (normalizedWeight > 0.01f && normalizedWeight < 0.99f) {
                                    float negNetScore = -(float) Math.log(1.0f / normalizedWeight - 1.0f);
                                    netScore = -negNetScore;
                                } else {
                                    netScore = -3.0f;  // 接近下限
                                }
                            } else {
                                netScore = 0.0f;  // 平衡
                            }

                            // 从净得分估算正负得分（简化：假设正负得分差为净得分，且较小的一方为0）
                            float positiveScore = netScore > 0 ? netScore : 0.0f;
                            float negativeScore = netScore < 0 ? -netScore : 0.0f;

                            tags.put(trimmedTag, new TagWithScoreAndTime(trimmedTag, positiveScore, negativeScore,
                                    currentTime, netScore > 0 ? 1 : 0, netScore < 0 ? 1 : 0));
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get feedback tags list: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOG.debug("Failed to read existing tags from Redis for uid {}: {}", uid, e.getMessage());
            }
            return tags;
        }

        /**
         * 从 Redis 读取现有作者（带时间信息）
         * 注意：由于我们只存储归一化后的权重，需要反向计算正负得分
         */
        private Map<Long, AuthorWithScoreAndTime> readExistingAuthorsFromRedis(long uid, long currentTime) {
            Map<Long, AuthorWithScoreAndTime> authors = new HashMap<>();
            try {
                String redisKey = PREFIX + uid + SUFFIX;
                CompletableFuture<org.apache.flink.api.java.tuple.Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(commands -> commands.get(redisKey));
                org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = valueFuture.get();
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    try {
                        List<RecFeature.FeedbackAuthorId> feedbackAuthorIds = feature.getFeedbackAuthorIdsList();
                        for (RecFeature.FeedbackAuthorId feedbackAuthorId : feedbackAuthorIds) {
                            long authorId = feedbackAuthorId.getAuthorId();
                            if (authorId <= 0) {
                                continue;
                            }
                            float normalizedWeight = feedbackAuthorId.getWeight();

                            // 从归一化权重反推净得分
                            float netScore = 0.0f;
                            if (normalizedWeight > 1.0f && normalizedWeight <= 2.0f) {
                                // 正反馈占优
                                float sigmoidValue = normalizedWeight - 1.0f;
                                if (sigmoidValue > 0.01f && sigmoidValue < 0.99f) {
                                    netScore = -(float) Math.log(1.0f / sigmoidValue - 1.0f);
                                } else {
                                    netScore = 3.0f;
                                }
                            } else if (normalizedWeight >= 0.0f && normalizedWeight < 1.0f) {
                                // 负反馈占优
                                if (normalizedWeight > 0.01f && normalizedWeight < 0.99f) {
                                    float negNetScore = -(float) Math.log(1.0f / normalizedWeight - 1.0f);
                                    netScore = -negNetScore;
                                } else {
                                    netScore = -3.0f;
                                }
                            } else {
                                netScore = 0.0f;
                            }

                            // 从净得分估算正负得分
                            float positiveScore = netScore > 0 ? netScore : 0.0f;
                            float negativeScore = netScore < 0 ? -netScore : 0.0f;

                            authors.put(authorId, new AuthorWithScoreAndTime(authorId, positiveScore, negativeScore,
                                    currentTime, netScore > 0 ? 1 : 0, netScore < 0 ? 1 : 0));
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get feedback author IDs list: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOG.debug("Failed to read existing authors from Redis for uid {}: {}", uid, e.getMessage());
            }
            return authors;
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
    }
}

