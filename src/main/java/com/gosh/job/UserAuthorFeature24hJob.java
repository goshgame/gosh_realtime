package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

public class UserAuthorFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserAuthorFeature24hJob.class);
    private static String PREFIX = "rec:user_author_feature:{";
    private static String SUFFIX = "}:post24h";
    // 每个窗口内每个用户的最大事件数限制
    private static final int MAX_EVENTS_PER_WINDOW = 2000;

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
//        env.setParallelism(1);

        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "post"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留观看事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(8))
            .name("Pre-filter View Events");

        // 3.1 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new ViewEventParser())
            .name("Parse View Events");

        // 3.2 将观看事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> unifiedStream = viewStream
            .flatMap(new ViewToFeatureMapper())
            .name("View to Feature")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            );

        // 第四步：按uid分组并进行24小时滑动窗口聚合
        DataStream<UserAuthorFeature24hAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureEvent, Long>() {
                @Override
                public Long getKey(UserFeatureEvent value) throws Exception {
                    return value.getUid();
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(24), // 窗口大小24小时
                Time.minutes(30) // 滑动间隔30分钟
            ))
            .aggregate(new UserAuthorFeature24hAggregator())
            .name("User-Author Feature 24h Aggregation");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, Map<String, byte[]>>> dataStream = aggregatedStream
            .map(new MapFunction<UserAuthorFeature24hAggregation, Tuple2<String, Map<String, byte[]>>>() {
                @Override
                public Tuple2<String, Map<String, byte[]>> map(UserAuthorFeature24hAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + SUFFIX;
                    return new Tuple2<>(redisKey, agg.authorFeatures);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(7200);
        redisConfig.setCommand("DEL_HMSET"); // 设置正确的命令用于HashMap操作
        RedisUtil.addRedisHashMapSink(
            dataStream,
            redisConfig,
            true,
            100
        );

        env.execute("User-Author Feature 24h Job");
    }

    public static class UserAuthorAccumulator {
        public long uid;
        public int totalEventCount = 0;
        // 按作者ID存储特征
        public Map<Long, AuthorFeatures> authorFeatureMap = new HashMap<>();

        public static class AuthorFeatures {
            // 观看
            public Set<String> view3sRecTokens = new HashSet<>();
            public Set<String> view8sRecTokens = new HashSet<>();
            public Set<String> view12sRecTokens = new HashSet<>();
            public Set<String> view20sRecTokens = new HashSet<>();
            // 停留
            public Set<String> stand5sRecTokens = new HashSet<>();
            public Set<String> stand10sRecTokens = new HashSet<>();
            // 互动
            public Set<String> likeRecTokens = new HashSet<>();
        }
    }

    public static class UserAuthorFeature24hAggregator implements AggregateFunction<UserFeatureEvent, UserAuthorAccumulator, UserAuthorFeature24hAggregation> {
        @Override
        public UserAuthorAccumulator createAccumulator() {
            UserAuthorAccumulator acc = new UserAuthorAccumulator();
            acc.totalEventCount = 0;
            return acc;
        }

        @Override
        public UserAuthorAccumulator add(UserFeatureEvent event, UserAuthorAccumulator acc) {
            // 先设置用户ID，确保即使跳过也能写入Redis
            acc.uid = event.uid;
            
            if (acc.totalEventCount >= MAX_EVENTS_PER_WINDOW) {
                // 如果超过限制，记录一次警告并跳过，但不影响现有数据的写入
                if (acc.totalEventCount == MAX_EVENTS_PER_WINDOW) {
                    LOG.warn("User {} has exceeded the event limit ({}). Further events will be skipped.", 
                            event.getUid(), MAX_EVENTS_PER_WINDOW);
                }
                return acc;
            }

            // 获取或创建作者特征对象
            UserAuthorAccumulator.AuthorFeatures authorFeatures = acc.authorFeatureMap.computeIfAbsent(event.author, 
                k -> new UserAuthorAccumulator.AuthorFeatures());

            if (event.progressTime >= 3) authorFeatures.view3sRecTokens.add(event.recToken);
            if (event.progressTime >= 8) authorFeatures.view8sRecTokens.add(event.recToken);
            if (event.progressTime >= 12) authorFeatures.view12sRecTokens.add(event.recToken);
            if (event.progressTime >= 20) authorFeatures.view20sRecTokens.add(event.recToken);

            if (event.standingTime >= 5) authorFeatures.stand5sRecTokens.add(event.recToken);
            if (event.standingTime >= 10) authorFeatures.stand10sRecTokens.add(event.recToken);

            if (event.interaction != null) {
                for (Integer it : event.interaction) {
                    if (it == 1) { // 点赞
                        authorFeatures.likeRecTokens.add(event.recToken);
                    }
                }
            }
            acc.totalEventCount++;
            return acc;
        }

        @Override
        public UserAuthorFeature24hAggregation getResult(UserAuthorAccumulator acc) {
            UserAuthorFeature24hAggregation result = new UserAuthorFeature24hAggregation();
            result.uid = acc.uid;
            result.authorFeatures = new HashMap<>();

            // 为每个作者生成特征protobuf
            for (Map.Entry<Long, UserAuthorAccumulator.AuthorFeatures> entry : acc.authorFeatureMap.entrySet()) {
                long authorId = entry.getKey();
                UserAuthorAccumulator.AuthorFeatures features = entry.getValue();

                byte[] featureBytes = RecFeature.RecUserAuthorFeature.newBuilder()
                    // 24小时观看特征
                    .setUserauthor3SviewCnt24H(features.view3sRecTokens.size())
                    .setUserauthor8SviewCnt24H(features.view8sRecTokens.size())
                    .setUserauthor12SviewCnt24H(features.view12sRecTokens.size())
                    .setUserauthor20SviewCnt24H(features.view20sRecTokens.size())
                    // 24小时停留特征
                    .setUserauthor5SstandCnt24H(features.stand5sRecTokens.size())
                    .setUserauthor10SstandCnt24H(features.stand10sRecTokens.size())
                    // 24小时互动特征
                    .setUserauthorLikeCnt24H(features.likeRecTokens.size())
                    .build()
                    .toByteArray();

                result.authorFeatures.put(String.valueOf(authorId), featureBytes);
            }
            
            result.updateTime = System.currentTimeMillis();
            return result;
        }

        @Override
        public UserAuthorAccumulator merge(UserAuthorAccumulator a, UserAuthorAccumulator b) {
            // 合并事件计数
            a.totalEventCount += b.totalEventCount;

            // 合并作者特征
            for (Map.Entry<Long, UserAuthorAccumulator.AuthorFeatures> entry : b.authorFeatureMap.entrySet()) {
                long authorId = entry.getKey();
                UserAuthorAccumulator.AuthorFeatures bFeatures = entry.getValue();
                UserAuthorAccumulator.AuthorFeatures aFeatures = a.authorFeatureMap.computeIfAbsent(authorId, 
                    k -> new UserAuthorAccumulator.AuthorFeatures());

                // 合并所有Set
                aFeatures.view3sRecTokens.addAll(bFeatures.view3sRecTokens);
                aFeatures.view8sRecTokens.addAll(bFeatures.view8sRecTokens);
                aFeatures.view12sRecTokens.addAll(bFeatures.view12sRecTokens);
                aFeatures.view20sRecTokens.addAll(bFeatures.view20sRecTokens);
                aFeatures.stand5sRecTokens.addAll(bFeatures.stand5sRecTokens);
                aFeatures.stand10sRecTokens.addAll(bFeatures.stand10sRecTokens);
                aFeatures.likeRecTokens.addAll(bFeatures.likeRecTokens);
            }
            return a;
        }
    }

    public static class UserAuthorFeature24hAggregation {
        public long uid;
        public Map<String, byte[]> authorFeatures; // authorId -> protobuf bytes
        public long updateTime;
    }
} 