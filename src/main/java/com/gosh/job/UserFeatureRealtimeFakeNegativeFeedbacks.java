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
 * 用户实时负反馈标签队列维护任务
 */
public class UserFeatureRealtimeFakeNegativeFeedbacks {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureRealtimeFakeNegativeFeedbacks.class);

    // Redis Key 前缀和后缀
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:latest5negtags";

    // 负反馈标签队列最大长度
    private static final int MAX_NEGATIVE_TAGS = 5;

    // 标签权重（统一为 0.1）
    private static final float TAG_WEIGHT = 0.1f;

    // Redis TTL（10分钟，单位：秒）
    private static final int REDIS_TTL = 10 * 60;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "gosh-negative-feedbacks";

    // 当 Redis 无法获取标签时的默认标签集合（包含 content 标签）
    private static final String DEFAULT_POST_TAGS =
            "age#youngadult,gender#male,object#human,content#news,quality#high,emotions#calm,formtype#commentary,"
                    + "language#english,occasion#formal,scenetype#indoor,appearance#bodytype,appearance#beauty_level,"
                    + "imagestyle#cinematic,restricted#clean,functiontype#knowledge";

    public static void main(String[] args) throws Exception {
        // 启动调试信息
        System.out.println("=== Flink任务启动 ===");
        System.out.println("启动时间: " + new Date());
        System.out.println("参数: " + Arrays.toString(args));

        try {
            // 第一步：创建 Flink 环境
            System.out.println("1. 创建Flink环境...");
            StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
            System.out.println("Flink环境创建完成，并行度: " + env.getParallelism());

            // 第二步：创建 Kafka Source
            System.out.println("2. 创建Kafka Source...");
            Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", KAFKA_GROUP_ID);
            System.out.println("Kafka配置: " + kafkaProperties);

            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post"
            );
            System.out.println("Kafka Source创建完成");

            // 第三步：使用 KafkaSource 创建 DataStream
            System.out.println("3. 创建Kafka数据流...");
            DataStreamSource<String> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                    "Kafka Source"
            );
            System.out.println("Kafka数据流创建完成");

            // 第四步：预过滤 - 只保留观看事件（event_type=8）
            System.out.println("4. 过滤观看事件...");
            DataStream<String> filteredStream = kafkaStream
                    .filter(EventFilterUtil.createFastEventTypeFilter(8))
                    .name("Pre-filter View Events")
                    .map(value -> {
                        System.out.println("收到Kafka原始消息: " + value);
                        return value;
                    })
                    .name("Debug Kafka Messages");

            // 第五步：解析观看事件
            System.out.println("5. 解析观看事件...");
            SingleOutputStreamOperator<UserFeatureCommon.PostViewEvent> viewStream = filteredStream
                    .flatMap(new UserFeatureCommon.ViewEventParser())
                    .name("Parse View Events")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("解析后的观看事件 - UID: " + event.uid +
                                    ", 信息数量: " + (event.infoList != null ? event.infoList.size() : 0));
                        } else {
                            System.out.println("解析观看事件返回null");
                        }
                        return event;
                    })
                    .name("Debug View Events");

            // 第六步：过滤负反馈事件（播放时长 < 3秒）
            System.out.println("6. 过滤负反馈事件...");
            SingleOutputStreamOperator<NegativeFeedbackEvent> negativeFeedbackStream = viewStream
                    .flatMap(new NegativeFeedbackEventParser())
                    .name("Filter Negative Feedback Events")
                    .map(event -> {
                        System.out.println("负反馈事件 - UID: " + event.uid +
                                ", PostID: " + event.postId +
                                ", 时间戳: " + event.timestamp);
                        return event;
                    })
                    .name("Debug Negative Feedback Events");

            // 第七步：从 Redis 获取视频标签并维护标签队列
            System.out.println("7. 设置窗口处理...");
            SingleOutputStreamOperator<UserNegativeTagQueue> tagQueueStream = negativeFeedbackStream
                    .keyBy(new KeySelector<NegativeFeedbackEvent, Long>() {
                        @Override
                        public Long getKey(NegativeFeedbackEvent value) throws Exception {
                            System.out.println("KeyBy - UID: " + value.uid);
                            return value.uid;
                        }
                    })
                    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))) // 10秒窗口，批量处理
                    .process(new NegativeTagQueueProcessor())
                    .name("Process Negative Tag Queue")
                    .map(queue -> {
                        System.out.println("标签队列处理完成 - UID: " + queue.uid +
                                ", 标签数量: " + queue.tags.size() +
                                ", 标签: " + queue.tags);
                        return queue;
                    })
                    .name("Debug Tag Queue");

            // 第八步：转换为 Protobuf 并写入 Redis
            System.out.println("8. 转换为Protobuf格式...");
            DataStream<Tuple2<String, byte[]>> dataStream = tagQueueStream
                    .map(new MapFunction<UserNegativeTagQueue, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(UserNegativeTagQueue queue) throws Exception {
                            // 构建 Redis key
                            String redisKey = PREFIX + queue.uid + SUFFIX;
                            System.out.println("构建Redis Key: " + redisKey);

                            // 构建 Protobuf
                            RecFeature.RecUserFeature.Builder builder = RecFeature.RecUserFeature.newBuilder();

                            // 添加负反馈标签
                            for (String tag : queue.tags) {
                                RecFeature.FeedbackTag.Builder tagBuilder = RecFeature.FeedbackTag.newBuilder();
                                tagBuilder.setTag(tag);
                                tagBuilder.setWeight(TAG_WEIGHT);
                                builder.addFeedbackTags(tagBuilder.build());
                            }

                            byte[] value = builder.build().toByteArray();
                            System.out.println("Protobuf序列化完成，数据大小: " + value.length + " bytes");
                            return new Tuple2<>(redisKey, value);
                        }
                    })
                    .name("Convert to Protobuf");

            // 第九步：创建 Redis Sink
            System.out.println("9. 创建Redis Sink...");
            RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            redisConfig.setTtl(REDIS_TTL);
            System.out.println("Redis配置: " + redisConfig);

            RedisUtil.addRedisSink(
                    dataStream,
                    redisConfig,
                    true, // 异步写入
                    100   // 批量大小
            );
            System.out.println("Redis Sink创建完成");

            // 执行任务
            System.out.println("=== 开始执行Flink任务 ===");
            System.out.println("执行时间: " + new Date());
            env.execute("User Feature Realtime Negative Feedbacks Job");

        } catch (Exception e) {
            System.err.println("!!! Flink任务执行异常 !!!");
            System.err.println("异常时间: " + new Date());
            e.printStackTrace();
            LOG.error("Flink任务执行失败", e);
            throw e;
        }

        System.out.println("=== Flink任务正常结束 ===");
        System.out.println("结束时间: " + new Date());
    }

    /**
     * 负反馈事件
     */
    public static class NegativeFeedbackEvent {
        public long uid;
        public long postId;
        public long timestamp;

        @Override
        public String toString() {
            return "NegativeFeedbackEvent{uid=" + uid + ", postId=" + postId + ", timestamp=" + timestamp + "}";
        }
    }

    /**
     * 用户负反馈标签队列
     */
    public static class UserNegativeTagQueue {
        public long uid;
        public List<String> tags; // 最多5个标签，按时间顺序（FIFO）

        @Override
        public String toString() {
            return "UserNegativeTagQueue{uid=" + uid + ", tags=" + tags + "}";
        }
    }

    /**
     * 负反馈事件解析器 - 增强调试版本
     */
    private static class NegativeFeedbackEventParser implements FlatMapFunction<UserFeatureCommon.PostViewEvent, NegativeFeedbackEvent> {
        private transient int processedCount = 0;

        @Override
        public void flatMap(UserFeatureCommon.PostViewEvent viewEvent, Collector<NegativeFeedbackEvent> out) throws Exception {
            processedCount++;

            if (viewEvent == null) {
                System.out.println("NegativeFeedbackEventParser: 输入事件为null");
                return;
            }

            if (viewEvent.infoList == null) {
                System.out.println("NegativeFeedbackEventParser: infoList为null, UID: " + viewEvent.uid);
                return;
            }

            System.out.println("NegativeFeedbackEventParser: 处理事件 " + processedCount +
                    ", UID: " + viewEvent.uid +
                    ", 信息数量: " + viewEvent.infoList.size());

            int negativeCount = 0;
            for (UserFeatureCommon.PostViewInfo info : viewEvent.infoList) {
                // 筛选播放时长 < 3秒的视频（负反馈信号）
                if (info.progressTime > 0 && info.progressTime < 3.0f && info.postId > 0) {
                    NegativeFeedbackEvent event = new NegativeFeedbackEvent();
                    event.uid = viewEvent.uid;
                    event.postId = info.postId;
                    event.timestamp = viewEvent.createdAt * 1000; // 转换为毫秒

                    System.out.println("发现负反馈事件 - UID: " + event.uid +
                            ", PostID: " + event.postId +
                            ", 播放时长: " + info.progressTime);

                    out.collect(event);
                    negativeCount++;
                }
            }

            if (negativeCount == 0) {
                System.out.println("NegativeFeedbackEventParser: 事件 " + processedCount + " 无负反馈信号");
            }
        }
    }

    /**
     * 负反馈标签队列处理器 - 增强调试版本
     */
    private static class NegativeTagQueueProcessor extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            NegativeFeedbackEvent, UserNegativeTagQueue, Long, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        private transient RedisConnectionManager redisConnectionManager;
        private transient RedisConfig redisConfig;
        private transient int windowCount = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("NegativeTagQueueProcessor: 初始化开始...");

            // 初始化 Redis 连接（用于读取视频标签）
            redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            System.out.println("NegativeTagQueueProcessor: Redis配置加载完成");

            redisConnectionManager = RedisConnectionManager.getInstance(redisConfig);
            System.out.println("NegativeTagQueueProcessor: Redis连接管理器初始化完成");

            LOG.info("NegativeTagQueueProcessor opened with Redis config: {}", redisConfig);
            System.out.println("NegativeTagQueueProcessor: open() 完成");
        }

        @Override
        public void close() throws Exception {
            System.out.println("NegativeTagQueueProcessor: 关闭，总共处理窗口: " + windowCount);
            if (redisConnectionManager != null) {
                redisConnectionManager.shutdown();
                System.out.println("NegativeTagQueueProcessor: Redis连接已关闭");
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

            windowCount++;
            System.out.println("=== 处理窗口 " + windowCount + " ===");
            System.out.println("窗口处理 - UID: " + uid +
                    ", 窗口时间: " + context.window().getStart() + " - " + context.window().getEnd());

            // 统计事件数量
            int eventCount = 0;
            for (NegativeFeedbackEvent event : elements) {
                eventCount++;
            }
            System.out.println("窗口内事件数量: " + eventCount);

            if (eventCount == 0) {
                System.out.println("窗口无事件，跳过处理");
                return;
            }

            // 读取用户现有的负反馈标签队列（从 Redis）
            System.out.println("读取现有标签队列...");
            LinkedHashSet<String> existingTags = readExistingTagsFromRedis(uid);
            System.out.println("现有标签数量: " + existingTags.size() + ", 内容: " + existingTags);

            // 处理窗口内的所有负反馈事件
            List<CompletableFuture<String>> tagFutures = new ArrayList<>();
            List<NegativeFeedbackEvent> events = new ArrayList<>();

            for (NegativeFeedbackEvent event : elements) {
                events.add(event);
                System.out.println("处理事件 - PostID: " + event.postId + ", 时间戳: " + event.timestamp);

                // 异步从 Redis 获取视频标签
                CompletableFuture<String> tagFuture = getPostTagFromRedis(event.postId);
                tagFutures.add(tagFuture);
            }

            System.out.println("等待获取 " + tagFutures.size() + " 个视频标签...");

            // 等待所有标签获取完成
            for (int i = 0; i < tagFutures.size(); i++) {
                try {
                    System.out.println("获取标签 " + (i+1) + "/" + tagFutures.size() + " - PostID: " + events.get(i).postId);
                    String tag = tagFutures.get(i).get();

                    if (tag != null && !tag.isEmpty()) {
                        System.out.println("获取到标签: " + tag + " for PostID: " + events.get(i).postId);

                        // 如果标签已存在，先移除（保持 FIFO 顺序）
                        boolean existed = existingTags.remove(tag);
                        if (existed) {
                            System.out.println("标签已存在，先移除: " + tag);
                        }

                        // 添加到队列末尾
                        existingTags.add(tag);
                        System.out.println("标签添加到队列: " + tag);

                        // 如果超过最大长度，移除最旧的标签（第一个）
                        while (existingTags.size() > MAX_NEGATIVE_TAGS) {
                            Iterator<String> iterator = existingTags.iterator();
                            if (iterator.hasNext()) {
                                String removedTag = iterator.next();
                                iterator.remove();
                                System.out.println("队列超限，移除最旧标签: " + removedTag);
                            }
                        }
                    } else {
                        System.out.println("未获取到标签 for PostID: " + events.get(i).postId);
                    }
                } catch (Exception e) {
                    System.err.println("获取标签失败 for PostID " + events.get(i).postId + ": " + e.getMessage());
                    LOG.warn("Failed to get tag for postId {}: {}", events.get(i).postId, e.getMessage());
                }
            }

            // 如果标签队列有更新，输出结果
            if (!existingTags.isEmpty()) {
                UserNegativeTagQueue queue = new UserNegativeTagQueue();
                queue.uid = uid;
                queue.tags = new ArrayList<>(existingTags);

                System.out.println("输出标签队列 - UID: " + uid +
                        ", 最终标签数量: " + queue.tags.size() +
                        ", 标签: " + queue.tags);

                out.collect(queue);
            } else {
                System.out.println("标签队列为空，无输出");
            }

            System.out.println("=== 窗口处理完成 ===");
        }

        /**
         * 从 Redis 读取用户现有的负反馈标签队列
         */
        private LinkedHashSet<String> readExistingTagsFromRedis(long uid) {
            LinkedHashSet<String> tags = new LinkedHashSet<>();
            try {
                String redisKey = PREFIX + uid + SUFFIX;
                System.out.println("读取Redis Key: " + redisKey);

                // 从 Redis 读取 Protobuf 数据
                CompletableFuture<Tuple2<String, byte[]>> valueFuture =
                        redisConnectionManager.executeStringAsync(
                                commands -> commands.get(redisKey)
                        );

                System.out.println("等待Redis读取完成...");
                Tuple2<String, byte[]> tuple = valueFuture.get();

                if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                    System.out.println("Redis读取成功，数据大小: " + tuple.f1.length + " bytes");

                    // 解析 Protobuf
                    RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(tuple.f1);
                    System.out.println("Protobuf解析成功");

                    // 注意：需要等 Protobuf 重新编译后，getFeedbackTagsList() 方法才会存在
                    // 暂时使用反射或直接访问字段
                    try {
                        java.lang.reflect.Method method = feature.getClass().getMethod("getFeedbackTagsList");
                        @SuppressWarnings("unchecked")
                        List<RecFeature.FeedbackTag> feedbackTags =
                                (List<RecFeature.FeedbackTag>) method.invoke(feature);

                        System.out.println("获取到反馈标签数量: " + feedbackTags.size());
                        for (RecFeature.FeedbackTag feedbackTag : feedbackTags) {
                            String tag = feedbackTag.getTag();
                            tags.add(tag);
                            System.out.println("现有标签: " + tag);
                        }
                    } catch (Exception e) {
                        System.err.println("反射获取标签列表失败: " + e.getMessage());
                        LOG.warn("Failed to get feedback tags list, may need to recompile Protobuf: {}", e.getMessage());
                    }
                } else {
                    System.out.println("Redis中无现有数据，可能是首次处理");
                }
            } catch (Exception e) {
                System.err.println("读取现有标签失败: " + e.getMessage());
                LOG.debug("Failed to read existing tags from Redis for uid {}: {}", uid, e.getMessage());
            }
            return tags;
        }

        /**
         * 获取视频标签：直接使用默认标签集合，从中提取 content 标签
         */
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            System.out.println("获取视频标签 - 使用默认标签集合 (PostID: " + postId + ")");
            return CompletableFuture.completedFuture(extractContentTag(DEFAULT_POST_TAGS));
        }

        /**
         * 从传入的标签字符串中提取第一个包含 content 的标签
         */
        private String extractContentTag(String tagString) {
            if (tagString == null || tagString.isEmpty()) {
                return null;
            }
            String[] tags = tagString.split(",");
            System.out.println("分割标签数量: " + tags.length);
            for (String tag : tags) {
                if (tag != null && tag.contains("content")) {
                    String result = tag.trim();
                    System.out.println("找到content标签: " + result);
                    return result;
                }
            }
            System.out.println("标签字符串中未包含content，返回第一个标签作为兜底");
            return tags.length > 0 ? tags[0].trim() : null;
        }
    }
}