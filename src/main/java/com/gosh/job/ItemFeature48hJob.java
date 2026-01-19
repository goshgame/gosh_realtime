package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.feature.RecFeature;
import com.gosh.job.AiTagParseCommon.PostInfoEvent;
import com.gosh.job.AiTagParseCommon.PostTagsEventParser;
import com.gosh.job.AiTagParseCommon.PostTagsToPostInfoMapper;
import com.gosh.job.ItemFeatureCommon.ItemFeatureAccumulator;
import com.gosh.job.UserFeatureCommon.ExposeEventParser;
import com.gosh.job.UserFeatureCommon.ExposeToFeatureMapper;
import com.gosh.job.UserFeatureCommon.PostExposeEvent;
import com.gosh.job.UserFeatureCommon.PostViewEvent;
import com.gosh.job.UserFeatureCommon.UserFeatureEvent;
import com.gosh.job.UserFeatureCommon.ViewEventParser;
import com.gosh.job.UserFeatureCommon.ViewToFeatureMapper;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ItemFeature48hJob {
    private static final Logger LOG = LoggerFactory.getLogger(ItemFeature48hJob.class);
    private static final String PREFIX = "rec:item_feature:{";
    private static final String SUFFIX = "}:post48h";

    // 48小时的毫秒数
    private static final long WINDOW_SIZE_MS = 48 * 60 * 60 * 1000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

        // 1. 交互流 Source (Topic: post)
        KafkaSource<String> postTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), "post");

        DataStreamSource<String> postSource = env.fromSource(
                postTopic,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withIdleness(Duration.ofMinutes(1)),
                "Post Kafka Source");

        // 1.1 解析交互流
        DataStream<UserFeatureEvent> interactionStream = postSource
                .filter(EventFilterUtil.createFastEventTypeFilter(16, 8))
                .flatMap(new ExposeEventParser())
                .flatMap(new ExposeToFeatureMapper())
                .union(
                        postSource
                                .filter(EventFilterUtil.createFastEventTypeFilter(8))
                                .flatMap(new ViewEventParser())
                                .flatMap(new ViewToFeatureMapper()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp()));

        // 2. 创建流 Source (Topic: rec)
        KafkaSource<String> recTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), "rec");

        DataStreamSource<String> recSource = env.fromSource(
                recTopic,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withIdleness(Duration.ofMinutes(5)),
                "Rec Kafka Source");

        // 2.1 解析创建流 (获取 PostInfoEvent.createdAt)
        DataStream<PostInfoEvent> creationStream = recSource
                .flatMap(new PostTagsEventParser()) // 解析 event_type=11
                .flatMap(new PostTagsToPostInfoMapper())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PostInfoEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, recordTimestamp) -> event.createdAt * 1000) // createdAt是秒，转毫秒
                );

        // 3. 双流 Connect 并处理
        SingleOutputStreamOperator<Tuple2<String, byte[]>> resultStream = interactionStream
                .connect(creationStream)
                .keyBy(
                        new KeySelector<UserFeatureEvent, Long>() {
                            @Override
                            public Long getKey(UserFeatureEvent value) {
                                return value.postId;
                            }
                        },
                        new KeySelector<PostInfoEvent, Long>() {
                            @Override
                            public Long getKey(PostInfoEvent value) {
                                return value.postId;
                            }
                        })
                .process(new Post48hCumulativeProcessFunction());

        // 4. Sink 到 Redis
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(7 * 24 * 60 * 60); // TTL 设置为 7 天，略大于 48h

        RedisUtil.addRedisSink(
                resultStream,
                redisConfig,
                true,
                100);

        env.execute("Item Feature 48h Cumulative Job");
    }

    /**
     * 自定义处理函数：统计 Post 创建后 48 小时内的累计特征
     */
    public static class Post48hCumulativeProcessFunction
            extends KeyedCoProcessFunction<Long, UserFeatureEvent, PostInfoEvent, Tuple2<String, byte[]>> {

        // 存储 Post 创建时间
        private ValueState<Long> createdAtState;
        // 存储累计特征
        private ValueState<ItemFeatureAccumulator> accumulatorState;
        // 标记是否已注册定时器
        private ValueState<Boolean> timerRegisteredState;

        @Override
        public void open(Configuration parameters) {
            createdAtState = getRuntimeContext().getState(new ValueStateDescriptor<>("createdAt", Long.class));
            accumulatorState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("accumulator", ItemFeatureAccumulator.class));
            timerRegisteredState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("timerRegistered", Boolean.class));
        }

        // 处理交互事件 (UserFeatureEvent)
        @Override
        public void processElement1(UserFeatureEvent event, Context ctx, Collector<Tuple2<String, byte[]>> out)
                throws Exception {
            Long createdAt = createdAtState.value();

            // 如果还没有收到创建时间，或者事件时间在 [createdAt, createdAt + 48h] 范围内
            // 注意：如果 createdAt 为空，我们暂时也进行累加，防止 rec 流延迟导致数据丢失。
            // 严格模式下可以缓存，但为了实时性通常选择先累加。
            if (createdAt == null || (event.timestamp >= createdAt && event.timestamp <= createdAt + WINDOW_SIZE_MS)) {

                ItemFeatureAccumulator acc = accumulatorState.value();
                if (acc == null) {
                    acc = new ItemFeatureAccumulator();
                    acc.postId = event.postId;
                }

                // 使用通用逻辑累加特征
                ItemFeatureCommon.addEventToAccumulator(event, acc);
                accumulatorState.update(acc);

                // 实时输出当前累计结果
                emitResult(acc, out);
            }
        }

        // 处理创建事件 (PostInfoEvent)
        @Override
        public void processElement2(PostInfoEvent event, Context ctx, Collector<Tuple2<String, byte[]>> out)
                throws Exception {
            // 更新创建时间
            long createdAtMillis = event.createdAt * 1000;
            createdAtState.update(createdAtMillis);

            // 注册 48 小时后的定时器，用于清理状态
            if (timerRegisteredState.value() == null) {
                long cleanupTime = createdAtMillis + WINDOW_SIZE_MS;
                ctx.timerService().registerEventTimeTimer(cleanupTime);
                timerRegisteredState.update(true);
            }
        }

        // 定时器触发：窗口结束，清理状态
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, byte[]>> out)
                throws Exception {
            // 48小时窗口结束，清理所有状态
            createdAtState.clear();
            accumulatorState.clear();
            timerRegisteredState.clear();
        }

        private void emitResult(ItemFeatureAccumulator acc, Collector<Tuple2<String, byte[]>> out) {
            String redisKey = PREFIX + acc.postId + SUFFIX;

            // 构建 Protobuf (复用 RecPostFeature，将 48h 数据映射到现有字段或假设已有新字段)
            // 这里为了演示，我们将 48h 的数据填充到 RecPostFeature 中
            // 注意：实际生产中建议在 .proto 文件中添加 _48h 后缀的字段
            RecFeature.RecPostFeature.Builder builder = RecFeature.RecPostFeature.newBuilder()
                    .setPostId(acc.postId)
                    // 曝光
                    .setPostExpCnt24H((int) acc.exposeHLL.cardinality())
                    // 观看
                    .setPost3SviewCnt24H((int) acc.view3sHLL.cardinality())
                    .setPost8SviewCnt24H((int) acc.view8sHLL.cardinality())
                    .setPost12SviewCnt24H((int) acc.view12sHLL.cardinality())
                    .setPost20SviewCnt24H((int) acc.view20sHLL.cardinality())
                    // 停留
                    .setPost5SstandCnt24H((int) acc.stand5sHLL.cardinality())
                    .setPost10SstandCnt24H((int) acc.stand10sHLL.cardinality())
                    // 互动
                    .setPostLikeCnt24H((int) acc.likeHLL.cardinality())
                    .setPostFollowCnt24H((int) acc.followHLL.cardinality())
                    .setPostProfileCnt24H((int) acc.profileHLL.cardinality())
                    .setPostPosinterCnt24H((int) acc.posinterHLL.cardinality());

            out.collect(new Tuple2<>(redisKey, builder.build().toByteArray()));
        }
    }

    /**
     * 48小时聚合结果 POJO (与 ItemFeature1hAggregation 结构保持一致)
     */
    public static class ItemFeature48hAggregation {
        public long postId;
        public int postExpCnt48h;
        public int post3sviewCnt48h;
        public int post8sviewCnt48h;
        public int post12sviewCnt48h;
        public int post20sviewCnt48h;
        public int post5sstandCnt48h;
        public int post10sstandCnt48h;
        public int postLikeCnt48h;
        public int postFollowCnt48h;
        public int postProfileCnt48h;
        public int postPosinterCnt48h;
    }
}
