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

public class UserAuthorFeature24hJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserAuthorFeature24hJob.class);
    private static String PREFIX = "rec:user_author_feature:{";
    private static String SUFFIX = "}:post24h";

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

        // 3.0 预过滤 - 只保留我们需要的事件类型
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(16, 8))
            .name("Pre-filter Events");

        // 3.1 解析曝光事件 (event_type=16)
        SingleOutputStreamOperator<PostExposeEvent> exposeStream = filteredStream
            .flatMap(new UserFeatureCommon.ExposeEventParser())
            .name("Parse Expose Events");

        // 3.2 解析观看事件 (event_type=8)
        SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
            .flatMap(new UserFeatureCommon.ViewEventParser())
            .name("Parse View Events");

        // 3.3 将曝光事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> exposeFeatureStream = exposeStream
            .flatMap(new UserFeatureCommon.ExposeToFeatureMapper())
            .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的用户特征事件
        DataStream<UserFeatureEvent> viewFeatureStream = viewStream
            .flatMap(new UserFeatureCommon.ViewToFeatureMapper())
            .name("View to Feature");

        // 3.5 合并两个流
        DataStream<UserFeatureEvent> unifiedStream = exposeFeatureStream
            .union(viewFeatureStream)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
            );

        // 第四步：按(uid, author)分组并进行24小时滑动窗口聚合
        DataStream<UserAuthorFeature24hAggregation> aggregatedStream = unifiedStream
            .keyBy(new KeySelector<UserFeatureEvent, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> getKey(UserFeatureEvent value) throws Exception {
                    return new Tuple2<>(value.getUid(), value.author);
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.hours(24), // 窗口大小24小时
                Time.minutes(30) // 滑动间隔30分钟
            ))
            .aggregate(new UserAuthorFeature24hAggregator())
            .name("User-Author Feature 24h Aggregation");

        // 打印聚合结果用于调试（采样）
//        aggregatedStream
//            .process(new ProcessFunction<UserAuthorFeature24hAggregation, UserAuthorFeature24hAggregation>() {
//                private static final long SAMPLE_INTERVAL = 60000; // 采样间隔1分钟
//                private static final int SAMPLE_COUNT = 3; // 每次采样3条
//                private transient long lastSampleTime;
//                private transient int sampleCount;
//
//                @Override
//                public void open(Configuration parameters) throws Exception {
//                    lastSampleTime = 0;
//                    sampleCount = 0;
//                }
//
//                @Override
//                public void processElement(UserAuthorFeature24hAggregation value, Context ctx, Collector<UserAuthorFeature24hAggregation> out) throws Exception {
//                    long now = System.currentTimeMillis();
//                    if (now - lastSampleTime > SAMPLE_INTERVAL) {
//                        lastSampleTime = now - (now % SAMPLE_INTERVAL);
//                        sampleCount = 0;
//                    }
//                    if (sampleCount < SAMPLE_COUNT) {
//                        sampleCount++;
//                        LOG.info("[Sample {}/{}] uid {} author {} at {}: exp24h={}, 3sview24h={}, 8sview24h={}, like24h={}",
//                            sampleCount,
//                            SAMPLE_COUNT,
//                            value.uid,
//                            value.author,
//                            new SimpleDateFormat("HH:mm:ss").format(new Date()),
//                            value.userauthorExpCnt24h,
//                            value.userauthor3sviewCnt24h,
//                            value.userauthor8sviewCnt24h,
//                            value.userauthorLikeCnt24h);
//                    }
//                }
//            })
//            .name("Debug Sampling");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .map(new MapFunction<UserAuthorFeature24hAggregation, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(UserAuthorFeature24hAggregation agg) throws Exception {
                    String redisKey = PREFIX + agg.uid + "_" + agg.author + SUFFIX;

                    byte[] value = RecFeature.RecUserAuthorFeature.newBuilder()
                        // 24小时曝光特征（当前无作者信息来源，置0）
                        .setUserauthorExpCnt24H(agg.userauthorExpCnt24h)
                        // 24小时观看特征
                        .setUserauthor3SviewCnt24H(agg.userauthor3sviewCnt24h)
                        .setUserauthor8SviewCnt24H(agg.userauthor8sviewCnt24h)
                        .setUserauthor12SviewCnt24H(agg.userauthor12sviewCnt24h)
                        .setUserauthor20SviewCnt24H(agg.userauthor20sviewCnt24h)
                        // 24小时停留特征
                        .setUserauthor5SstandCnt24H(agg.userauthor5sstandCnt24h)
                        .setUserauthor10SstandCnt24H(agg.userauthor10sstandCnt24h)
                        // 24小时互动特征
                        .setUserauthorLikeCnt24H(agg.userauthorLikeCnt24h)
                        .setExpireTime("24h")
                        .build()
                        .toByteArray();

                    return new Tuple2<>(redisKey, value);
                }
            })
            .name("Aggregation to Protobuf Bytes");

        // 第六步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(7200);
        RedisUtil.addRedisSink(
            dataStream,
            redisConfig,
            false,
            100
        );

        env.execute("User-Author Feature 24h Job");
    }

    public static class UserAuthorAccumulator {
        public long uid;
        public long author;
        public Set<String> exposeRecTokens = new HashSet<>();
        public Set<String> view3sRecTokens = new HashSet<>();
        public Set<String> view8sRecTokens = new HashSet<>();
        public Set<String> view12sRecTokens = new HashSet<>();
        public Set<String> view20sRecTokens = new HashSet<>();
        public Set<String> stand5sRecTokens = new HashSet<>();
        public Set<String> stand10sRecTokens = new HashSet<>();
        public Set<String> likeRecTokens = new HashSet<>();
    }

    public static class UserAuthorFeature24hAggregator implements AggregateFunction<UserFeatureEvent, UserAuthorAccumulator, UserAuthorFeature24hAggregation> {
        @Override
        public UserAuthorAccumulator createAccumulator() {
            return new UserAuthorAccumulator();
        }

        @Override
        public UserAuthorAccumulator add(UserFeatureEvent event, UserAuthorAccumulator acc) {
            acc.uid = event.uid;
            acc.author = event.author;
            if ("expose".equals(event.eventType)) {
                // 暂无author信息，无法归因到作者层面
            }
            if ("view".equals(event.eventType)) {
                if (event.progressTime >= 3) acc.view3sRecTokens.add(event.recToken);
                if (event.progressTime >= 8) acc.view8sRecTokens.add(event.recToken);
                if (event.progressTime >= 12) acc.view12sRecTokens.add(event.recToken);
                if (event.progressTime >= 20) acc.view20sRecTokens.add(event.recToken);

                if (event.standingTime >= 5) acc.stand5sRecTokens.add(event.recToken);
                if (event.standingTime >= 10) acc.stand10sRecTokens.add(event.recToken);

                if (event.interaction != null) {
                    for (Integer it : event.interaction) {
                        if (it == 1) { // 点赞
                            acc.likeRecTokens.add(event.recToken);
                        }
                    }
                }
            }
            return acc;
        }

        @Override
        public UserAuthorFeature24hAggregation getResult(UserAuthorAccumulator acc) {
            UserAuthorFeature24hAggregation r = new UserAuthorFeature24hAggregation();
            r.uid = acc.uid;
            r.author = acc.author;
            r.userauthorExpCnt24h = acc.exposeRecTokens.size(); // 目前为0，因缺author
            r.userauthor3sviewCnt24h = acc.view3sRecTokens.size();
            r.userauthor8sviewCnt24h = acc.view8sRecTokens.size();
            r.userauthor12sviewCnt24h = acc.view12sRecTokens.size();
            r.userauthor20sviewCnt24h = acc.view20sRecTokens.size();
            r.userauthor5sstandCnt24h = acc.stand5sRecTokens.size();
            r.userauthor10sstandCnt24h = acc.stand10sRecTokens.size();
            r.userauthorLikeCnt24h = acc.likeRecTokens.size();
            r.updateTime = System.currentTimeMillis();
            return r;
        }

        @Override
        public UserAuthorAccumulator merge(UserAuthorAccumulator a, UserAuthorAccumulator b) {
            a.exposeRecTokens.addAll(b.exposeRecTokens);
            a.view3sRecTokens.addAll(b.view3sRecTokens);
            a.view8sRecTokens.addAll(b.view8sRecTokens);
            a.view12sRecTokens.addAll(b.view12sRecTokens);
            a.view20sRecTokens.addAll(b.view20sRecTokens);
            a.stand5sRecTokens.addAll(b.stand5sRecTokens);
            a.stand10sRecTokens.addAll(b.stand10sRecTokens);
            a.likeRecTokens.addAll(b.likeRecTokens);
            return a;
        }
    }

    public static class UserAuthorFeature24hAggregation {
        public long uid;
        public long author;
        public int userauthorExpCnt24h;
        public int userauthor3sviewCnt24h;
        public int userauthor8sviewCnt24h;
        public int userauthor12sviewCnt24h;
        public int userauthor20sviewCnt24h;
        public int userauthor5sstandCnt24h;
        public int userauthor10sstandCnt24h;
        public int userauthorLikeCnt24h;
        public long updateTime;
    }
} 