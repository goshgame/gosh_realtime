package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.feature.RecFeature;
import com.gosh.job.RecValidPostParseCommon.RecValidPostEvent;
import com.gosh.job.ItemFeatureCommon.ItemFeatureAccumulator;
import com.gosh.job.UserFeatureCommon.ExposeEventParser;
import com.gosh.job.UserFeatureCommon.ExposeToFeatureMapper;
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
        private static final int keepEventType = 17;

        // 48小时的毫秒数
        // 窗口大小
        private static final long WINDOW_SIZE_MS = 48 * 60 * 60 * 1000L;

        public static void main(String[] args) throws Exception {
                LOG.info("ItemFeature48hJob start");
                // 创建flink环境
                StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

                // 1. 交互流 Source (Topic: post)
                KafkaSource<String> postTopic = KafkaEnvUtil.createKafkaSource(
                                KafkaEnvUtil.loadProperties(), "post");

                DataStreamSource<String> postSource = env.fromSource(
                                postTopic,
                                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                                .withIdleness(Duration.ofMinutes(5)),
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
                                                WatermarkStrategy
                                                                .<UserFeatureEvent>forBoundedOutOfOrderness(
                                                                                Duration.ofSeconds(10))
                                                                .withTimestampAssigner((event, recordTimestamp) -> event
                                                                                .getTimestamp()));

                // 2. 创建流 Source (Topic: rec)
                KafkaSource<String> recTopic = KafkaEnvUtil.createKafkaSource(
                                KafkaEnvUtil.loadProperties(), "rec");

                DataStreamSource<String> recSource = env.fromSource(
                                recTopic,
                                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                                "Rec Kafka Source");

                DataStream<String> filteredRecStream = recSource
                                .filter(EventFilterUtil.createFastEventTypeFilter(keepEventType)) // event_type=17
                                .name("Pre-filter Events");

                // 2.1 解析创建流 (获取 RecValidPostEvent.taggingAt 作为 Post 的起始时间)
                DataStream<RecValidPostEvent> creationStream = filteredRecStream
                                .flatMap(new RecValidPostParseCommon.RecValidPostEventParser()) // 解析为 RecValidPostEvent
                                .name("Parse RecValidPostEvents");

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
                                                new KeySelector<RecValidPostEvent, Long>() {
                                                        @Override
                                                        public Long getKey(RecValidPostEvent value) {
                                                                return value.id;
                                                        }
                                                })
                                .process(new Post48hCumulativeProcessFunction());

                // 4. Sink 到 Redis
                RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
                // TODO: 测试时间一个小时，需要修改
                redisConfig.setTtl(7 * 24 * 60 * 60); // TTL 设置为 7 天，略大于 48h
                // redisConfig.setTtl(1 * 60 * 60); // TTL 设置为 1 小时

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
                        extends
                        KeyedCoProcessFunction<Long, UserFeatureEvent, RecValidPostEvent, Tuple2<String, byte[]>> {
                // 存储 Post 的起始时间（taggingAt），作为 48 小时窗口的起点
                private ValueState<Long> postStartTimeState;
                // 存储累计特征 (ItemFeatureAccumulator 及其中的 HyperLogLog 必须是可序列化的，
                // 且 ItemFeatureAccumulator 必须是 Flink 能够序列化的 POJO)
                private ValueState<ItemFeatureAccumulator> accumulatorState;
                // 存储48小时清理定时器的时间戳
                private ValueState<Long> cleanupTimerState;

                @Override
                public void open(Configuration parameters) {
                        postStartTimeState = getRuntimeContext()
                                        .getState(new ValueStateDescriptor<>("createdAt", Long.class));
                        accumulatorState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("accumulator", ItemFeatureAccumulator.class));
                        cleanupTimerState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("cleanupTimer", Long.class));
                }

                // 处理交互事件 (UserFeatureEvent)
                @Override
                public void processElement1(UserFeatureEvent event, Context ctx, Collector<Tuple2<String, byte[]>> out)
                                throws Exception {
                        Long postStartTime = postStartTimeState.value();
                        // 只有当 Post 的起始时间（taggingAt）已知时才处理交互事件
                        // 且事件时间必须在 [postStartTime, postStartTime + 48h] 范围内
                        if (postStartTime != null && event.timestamp >= postStartTime
                                        && event.timestamp <= postStartTime + WINDOW_SIZE_MS) {

                                LOG.info("processElement1: acc event={}, timestamp={}, postStartTime={}, postEndTime={}",
                                                event.postId, event.timestamp, postStartTime,
                                                postStartTime + WINDOW_SIZE_MS);
                                ItemFeatureAccumulator acc = accumulatorState.value();
                                if (acc == null) {
                                        acc = new ItemFeatureAccumulator();
                                        acc.postId = event.postId;
                                }

                                // 使用通用逻辑累加特征
                                ItemFeatureCommon.addEventToAccumulator(event, acc);
                                accumulatorState.update(acc);
                                emitResult(acc, out);
                        }
                }

                // 处理创建事件 (RecValidPostEvent)
                @Override
                public void processElement2(RecValidPostEvent event, Context ctx, Collector<Tuple2<String, byte[]>> out)
                                throws Exception {
                        // 更新创建时间
                        long createdAtMillis = event.taggingAt * 1000;
                        postStartTimeState.update(createdAtMillis);
                        LOG.info("processElement2: postId {} createdAtMillis={}", ctx.getCurrentKey(),
                                        createdAtMillis);

                        // 注册 48 小时后的定时器，用于清理状态
                        if (cleanupTimerState.value() == null) {
                                long cleanupTime = createdAtMillis + WINDOW_SIZE_MS;
                                ctx.timerService().registerEventTimeTimer(cleanupTime);
                                cleanupTimerState.update(cleanupTime);
                                LOG.error("processElement2 Registered event time cleanup timer for postId {} at {}",
                                                ctx.getCurrentKey(),
                                                cleanupTime);
                        }
                }

                // 定时器触发：区分处理时间定时器（刷新）和事件时间定时器（清理）
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, byte[]>> out)
                                throws Exception {

                        Long cleanupTime = cleanupTimerState.value();
                        Long postStartTime = postStartTimeState.value();
                        LOG.error("onTimer: postId {} timestamp={}, cleanupTime={}, postStartTime={}",
                                        ctx.getCurrentKey(),
                                        timestamp, cleanupTime, postStartTime);
                        if (cleanupTime != null && timestamp == cleanupTime) {
                                // EventTime定时器触发，表示48小时窗口结束
                                LOG.error("onTimer Event time cleanup timer fired for postId {} at {}. Performing final flush and cleanup.",
                                                ctx.getCurrentKey(), timestamp);

                                // 在清理前执行最后一次写入
                                ItemFeatureAccumulator acc = accumulatorState.value(); // 获取最终累加器
                                if (acc != null) {
                                        emitResult(acc, out);
                                }

                                // 清理所有状态
                                postStartTimeState.clear();
                                accumulatorState.clear();
                                cleanupTimerState.clear(); // 清理事件时间定时器状态
                        }
                }

                private void emitResult(ItemFeatureAccumulator acc, Collector<Tuple2<String, byte[]>> out) {
                        String redisKey = PREFIX + acc.postId + SUFFIX;
                        // 构建 Protobuf，使用 48h 特征字段
                        RecFeature.RecPostFeature.Builder builder = RecFeature.RecPostFeature.newBuilder()
                                        .setPostId(acc.postId)
                                        // 曝光
                                        .setPostExpCnt48H((int) acc.exposeHLL.cardinality())
                                        // 观看
                                        .setPost3SviewCnt48H((int) acc.view3sHLL.cardinality())
                                        .setPost8SviewCnt48H((int) acc.view8sHLL.cardinality())
                                        .setPost12SviewCnt48H((int) acc.view12sHLL.cardinality())
                                        .setPost20SviewCnt48H((int) acc.view20sHLL.cardinality())
                                        // 停留
                                        .setPost5SstandCnt48H((int) acc.stand5sHLL.cardinality())
                                        .setPost10SstandCnt48H((int) acc.stand10sHLL.cardinality())
                                        // 互动
                                        .setPostLikeCnt48H((int) acc.likeHLL.cardinality())
                                        .setPostFollowCnt48H((int) acc.followHLL.cardinality())
                                        .setPostProfileCnt48H((int) acc.profileHLL.cardinality())
                                        .setPostPosinterCnt48H((int) acc.posinterHLL.cardinality());

                        out.collect(new Tuple2<>(redisKey, builder.build().toByteArray()));
                        LOG.info("post 48h emitResult: key={}, value={}", redisKey, builder.build().toString());
                }
        }

}
