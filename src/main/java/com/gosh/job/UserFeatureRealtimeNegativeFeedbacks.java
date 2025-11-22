package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon;
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
 *
 * 功能说明：
 * 1. 从 Kafka 消费用户行为日志
 * 2. 筛选播放时长 < 3秒的视频作为负反馈信号
 * 3. 从 Redis 获取该视频的标签信息（rec_post:{post_id}:aitag）
 * 4. 维护每个用户最近5个负反馈标签的队列（FIFO + 去重）
 * 5. 将标签队列以 Protobuf 格式写入 Redis（rec:user_feature:{uid}:latest5negtags）
 */
public class UserFeatureRealtimeNegativeFeedbacks {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureRealtimeNegativeFeedbacks.class);

    // Redis Key 前缀和后缀
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:latest5negtags";

    // 负反馈标签队列最大长度
    private static final int MAX_NEGATIVE_TAGS = 5;

    // 标签权重（统一为 0.1）
    private static final float TAG_WEIGHT = 0.1f;

    // Redis TTL（24小时，单位：秒）
    private static final int REDIS_TTL = 24 * 3600;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "gosh-negative-feedbacks";

    public static void main(String[] args) throws Exception {
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

        // 第六步：过滤负反馈事件（播放时长 < 3秒）
        SingleOutputStreamOperator<NegativeFeedbackEvent> negativeFeedbackStream = viewStream
                .flatMap(new NegativeFeedbackEventParser())
                .name("Filter Negative Feedback Events");

        // 第七步：从 Redis 获取视频标签并维护标签队列
        SingleOutputStreamOperator<UserNegativeTagQueue> tagQueueStream = negativeFeedbackStream
                .keyBy(new KeySelector<NegativeFeedbackEvent, Long>() {
                    @Override
                    public Long getKey(NegativeFeedbackEvent value) throws Exception {
                        return value.uid;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(java.time.Duration.ofSeconds(10))) // 10秒窗口，批量处理
                .process(new NegativeTagQueueProcessor())
                .name("Process Negative Tag Queue");

        // 第八步：转换为 Protobuf 并写入 Redis
        DataStream<Tuple2<String, byte[]>> dataStream = tagQueueStream
                .map(new MapFunction<UserNegativeTagQueue, Tuple2<String, byte[]>>() {
                    @Override
                    public Tuple2<String, byte[]> map(UserNegativeTagQueue queue) throws Exception {
                        // 构建 Redis key
                        String redisKey = PREFIX + queue.uid + SUFFIX;

                        // 构建 Protobuf
                        RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();

                        // 添加负反馈标签
                        for (String tag : queue.tags) {
                            RecFeature.FeedbackTag.Builder tagBuilder = RecFeature.FeedbackTag.newBuilder();
                            tagBuilder.setTag(tag);
                            tagBuilder.setWeight((int)(TAG_WEIGHT * 1000)); // fixed32 需要整数，0.1 * 1000 = 100
                            builder.addFeedbackTags(tagBuilder.build());
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
    }

    /**
     * 负反馈事件
     */
    public static class NegativeFeedbackEvent {
        public long uid;
        public long postId;
        public long timestamp;
    }

    /**
     * 用户负反馈标签队列
     */
    public static class UserNegativeTagQueue {
        public long uid;
        public List<String> tags; // 最多5个标签，按时间顺序（FIFO）
    }

    /**
     * 负反馈事件解析器
     * 从观看事件中筛选播放时长 < 3秒的视频
     */
    private static class NegativeFeedbackEventParser implements FlatMapFunction<UserFeatureCommon.PostViewEvent, NegativeFeedbackEvent> {
        @Override
        public void flatMap(UserFeatureCommon.PostViewEvent viewEvent, Collector<NegativeFeedbackEvent> out) throws Exception {
            if (viewEvent == null || viewEvent.infoList == null) {
                return;
            }

            for (UserFeatureCommon.PostViewInfo info : viewEvent.infoList) {
                // 筛选播放时长 < 3秒的视频（负反馈信号）
                if (info.progressTime > 0 && info.progressTime < 3.0f && info.postId > 0) {
                    NegativeFeedbackEvent event = new NegativeFeedbackEvent();
                    event.uid = viewEvent.uid;
                    event.postId = info.postId;
                    event.timestamp = viewEvent.createdAt * 1000; // 转换为毫秒
                    out.collect(event);
                }
            }
        }
    }

    /**
     * 负反馈标签队列处理器
     * 从 Redis 获取视频标签，维护每个用户最近5个负反馈标签的队列
     */
    private static class NegativeTagQueueProcessor extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            NegativeFeedbackEvent, UserNegativeTagQueue, Long, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        private transient RedisConnectionManager redisConnectionManager;
        private transient RedisConfig redisConfig;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化 Redis 连接（用于读取视频标签）
            redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConnectionManager = RedisConnectionManager.getInstance(redisConfig);
            LOG.info("NegativeTagQueueProcessor opened with Redis config: {}", redisConfig);
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
                                    NegativeFeedbackEvent, UserNegativeTagQueue, Long,
                                    org.apache.flink.streaming.api.windowing.windows.TimeWindow>.Context context,
                            Iterable<NegativeFeedbackEvent> elements,
                            Collector<UserNegativeTagQueue> out) throws Exception {

            // 读取用户现有的负反馈标签队列（从 Redis）
            LinkedHashSet<String> existingTags = readExistingTagsFromRedis(uid);

            // 处理窗口内的所有负反馈事件
            List<CompletableFuture<String>> tagFutures = new ArrayList<>();
            List<NegativeFeedbackEvent> events = new ArrayList<>();

            for (NegativeFeedbackEvent event : elements) {
                events.add(event);
                // 异步从 Redis 获取视频标签
                CompletableFuture<String> tagFuture = getPostTagFromRedis(event.postId);
                tagFutures.add(tagFuture);
            }

            // 等待所有标签获取完成
            for (int i = 0; i < tagFutures.size(); i++) {
                try {
                    String tag = tagFutures.get(i).get();
                    if (tag != null && !tag.isEmpty()) {
                        // 如果标签已存在，先移除（保持 FIFO 顺序）
                        existingTags.remove(tag);
                        // 添加到队列末尾
                        existingTags.add(tag);

                        // 如果超过最大长度，移除最旧的标签（第一个）
                        while (existingTags.size() > MAX_NEGATIVE_TAGS) {
                            Iterator<String> iterator = existingTags.iterator();
                            if (iterator.hasNext()) {
                                iterator.next();
                                iterator.remove();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to get tag for postId {}: {}", events.get(i).postId, e.getMessage());
                }
            }

            // 如果标签队列有更新，输出结果
            if (!existingTags.isEmpty()) {
                UserNegativeTagQueue queue = new UserNegativeTagQueue();
                queue.uid = uid;
                queue.tags = new ArrayList<>(existingTags);
                out.collect(queue);
            }
        }

        /**
         * 从 Redis 读取用户现有的负反馈标签队列
         */
        private LinkedHashSet<String> readExistingTagsFromRedis(long uid) {
            LinkedHashSet<String> tags = new LinkedHashSet<>();
            try {
                String redisKey = PREFIX + uid + SUFFIX;

                // 从 Redis 读取 Protobuf 数据
                // Redis get 返回 Tuple2<String, byte[]>，其中 f1 是值的字节数组
                CompletableFuture<org.apache.flink.api.java.tuple.Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(
                                commands -> commands.get(redisKey)
                        );

                org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = valueFuture.get();
                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    // 解析 Protobuf
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    // 注意：需要等 Protobuf 重新编译后，getFeedbackTagsList() 方法才会存在
                    // 暂时使用反射或直接访问字段
                    try {
                        java.lang.reflect.Method method = feature.getClass().getMethod("getFeedbackTagsList");
                        @SuppressWarnings("unchecked")
                        java.util.List<RecFeature.FeedbackTag> feedbackTags =
                                (java.util.List<RecFeature.FeedbackTag>) method.invoke(feature);
                        for (RecFeature.FeedbackTag feedbackTag : feedbackTags) {
                            tags.add(feedbackTag.getTag());
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to get feedback tags list, may need to recompile Protobuf: {}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                // 如果读取失败，返回空队列（可能是首次写入）
                LOG.debug("Failed to read existing tags from Redis for uid {}: {}", uid, e.getMessage());
            }
            return tags;
        }

        /**
         * 从 Redis 获取视频标签
         * Redis Key: rec_post:{post_id}:aitag
         * Value 格式: "age#youngadult,gender#male,..."
         * 返回第一个逗号分隔的元素
         */
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            String redisKey = "rec_post:" + postId + ":aitag";

            return redisConnectionManager.executeStringAsync(
                    commands -> {
                        // Redis get 返回 Tuple2<String, byte[]>，其中 f1 是值的字节数组
                        org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = commands.get(redisKey);
                        if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                            // 将字节数组转换为字符串
                            String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                            if (!value.isEmpty()) {
                                // 取第一个逗号分隔的元素
                                int commaIndex = value.indexOf(',');
                                if (commaIndex > 0) {
                                    return value.substring(0, commaIndex);
                                } else {
                                    return value;
                                }
                            }
                        }
                        return null;
                    }
            );
        }
    }
}

