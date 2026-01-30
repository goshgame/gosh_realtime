package com.gosh.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.FeastRequest;
import com.gosh.feature.RecFeature;
import com.gosh.job.UserFeatureCommon.*;
import com.gosh.job.ItemFeatureCommon.*;
import com.gosh.process.FeastApi;
import com.gosh.util.FeastSinkFunction;
import com.gosh.util.FlinkMonitorUtil;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemFeature1hJob {
    private static final Logger LOG = LoggerFactory.getLogger(ItemFeature1hJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String PREFIX = "rec:item_feature:{";
    private static String SUFFIX = "}:post1h";

    // 定义迟到数据的侧输出标签
    private static final OutputTag<UserFeatureEvent> LATE_DATA_TAG = new OutputTag<UserFeatureEvent>("late-data") {
    };

    public static void main(String[] args) throws Exception {
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        // env.setParallelism(3);

        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), "post");

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
                inputTopic,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withIdleness(Duration.ofMinutes(5)),
                "Kafka Source");

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

        // 3.3 将曝光事件转换为统一的特征事件
        DataStream<UserFeatureEvent> exposeFeatureStream = exposeStream
                .flatMap(new UserFeatureCommon.ExposeToFeatureMapper())
                .name("Expose to Feature");

        // 3.4 将观看事件转换为统一的特征事件
        DataStream<UserFeatureEvent> viewFeatureStream = viewStream
                .flatMap(new UserFeatureCommon.ViewToFeatureMapper())
                .name("View to Feature");

        // 3.5 合并两个流
        DataStream<UserFeatureEvent> unifiedStream = exposeFeatureStream
                .union(viewFeatureStream)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
                                .withIdleness(Duration.ofMinutes(5)));

        // 第四步：按post_id分组并进行1小时滑动窗口聚合
        SingleOutputStreamOperator<ItemFeature1hAggregation> aggregatedStream = unifiedStream
                .keyBy(new KeySelector<UserFeatureEvent, Long>() {
                    @Override
                    public Long getKey(UserFeatureEvent value) throws Exception {
                        return value.postId;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(
                        Time.hours(1), // 窗口大小1小时
                        Time.minutes(10) // 滑动间隔10分钟
                ))
                .aggregate(new ItemFeature1hAggregator())
                .name("Item Feature 1h Aggregation");

        // // 打印聚合结果用于调试（采样）
        // aggregatedStream
        // .process(new ProcessFunction<ItemFeature1hAggregation,
        // ItemFeature1hAggregation>() {
        // private static final long SAMPLE_INTERVAL = 60000; // 采样间隔1分钟
        // private static final int SAMPLE_COUNT = 3; // 每次采样3条
        // private transient long lastSampleTime;
        // private transient int sampleCount;
        //
        // @Override
        // public void open(Configuration parameters) throws Exception {
        // lastSampleTime = 0;
        // sampleCount = 0;
        // }
        //
        // @Override
        // public void processElement(ItemFeature1hAggregation value, Context ctx,
        // Collector<ItemFeature1hAggregation> out) throws Exception {
        // long now = System.currentTimeMillis();
        // if (now - lastSampleTime > SAMPLE_INTERVAL) {
        // lastSampleTime = now - (now % SAMPLE_INTERVAL);
        // sampleCount = 0;
        // }
        // if (sampleCount < SAMPLE_COUNT) {
        // sampleCount++;
        // LOG.info("[Sample {}/{}] postId {} at {}: exp1h={}, 3sview1h={}, 8sview1h={},
        // like1h={}",
        // sampleCount,
        // SAMPLE_COUNT,
        // value.postId,
        // new SimpleDateFormat("HH:mm:ss").format(new Date()),
        // value.postExpCnt1h,
        // value.post3sviewCnt1h,
        // value.post8sviewCnt1h,
        // value.postLikeCnt1h);
        // }
        // }
        // })
        // .name("Debug Sampling");

        // 第五步：转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
                .map(new MapFunction<ItemFeature1hAggregation, Tuple2<String, byte[]>>() {
                    @Override
                    public Tuple2<String, byte[]> map(ItemFeature1hAggregation agg) throws Exception {
                        // 构建Redis key
                        String redisKey = PREFIX + agg.postId + SUFFIX;

                        // 构建Protobuf
                        byte[] value = RecFeature.RecPostFeature.newBuilder()
                                // 1小时曝光特征
                                .setPostExpCnt1H(agg.postExpCnt1h)
                                // 1小时观看特征
                                .setPost3SviewCnt1H(agg.post3sviewCnt1h)
                                .setPost8SviewCnt1H(agg.post8sviewCnt1h)
                                .setPost12SviewCnt1H(agg.post12sviewCnt1h)
                                .setPost20SviewCnt1H(agg.post20sviewCnt1h)
                                // 1小时停留特征
                                .setPost5SstandCnt1H(agg.post5sstandCnt1h)
                                .setPost10SstandCnt1H(agg.post10sstandCnt1h)
                                // 1小时互动特征
                                .setPostLikeCnt1H(agg.postLikeCnt1h)
                                .setPostFollowCnt1H(agg.postFollowCnt1h)
                                .setPostProfileCnt1H(agg.postProfileCnt1h)
                                .setPostPosinterCnt1H(agg.postPosinterCnt1h)
                                .build()
                                .toByteArray();

                        return new Tuple2<>(redisKey, value);
                    }
                })
                .name("Aggregation to Protobuf Bytes");

        // 第六步：转换为FeastRequest并写入Feast
        SingleOutputStreamOperator<FeastRequest> feastRequestStream = aggregatedStream
                .map(new MapFunction<ItemFeature1hAggregation, FeastRequest>() {
                    @Override
                    public FeastRequest map(ItemFeature1hAggregation agg) throws Exception {
                        FeastRequest feastRequest = new FeastRequest();
                        feastRequest.setProject("gosh_realtime_feature_store"); // 可配置
                        feastRequest.setFeatureViewName("item_feature_post_1h"); // 特定于此作业

                        FeastRequest.FeastData feastData = new FeastRequest.FeastData();
                        FeastRequest.EntityKey entityKey = new FeastRequest.EntityKey();
                        entityKey.setJoinKeys(List.of("item_id"));
                        entityKey.setEntityValues(List.of(agg.postId));
                        feastData.setEntityKey(entityKey);

                        Map<String, FeastRequest.FeatureValue> features = new HashMap<>();
                        // 添加所有聚合特征
                        FeastApi.addFeature(features, "post_exp_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.postExpCnt1h);
                        FeastApi.addFeature(features, "post_3sview_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post3sviewCnt1h);
                        FeastApi.addFeature(features, "post_8sview_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post8sviewCnt1h);
                        FeastApi.addFeature(features, "post_12sview_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post12sviewCnt1h);
                        FeastApi.addFeature(features, "post_20sview_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post20sviewCnt1h);
                        FeastApi.addFeature(features, "post_5sstand_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post5sstandCnt1h);
                        FeastApi.addFeature(features, "post_10sstand_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.post10sstandCnt1h);
                        FeastApi.addFeature(features, "post_like_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.postLikeCnt1h);
                        FeastApi.addFeature(features, "post_follow_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.postFollowCnt1h);
                        FeastApi.addFeature(features, "post_profile_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.postProfileCnt1h);
                        FeastApi.addFeature(features, "post_posinter_cnt_1h", FeastRequest.ValueType.INT64,
                                (long) agg.postPosinterCnt1h);
                        feastData.setFeatures(features);
                        feastData.setEventTimestamp(agg.updateTime); // 使用聚合时间

                        feastRequest.setData(List.of(feastData));
                        feastRequest.setTtl(60); // 1分钟TTL，可配置

                        LOG.debug("Generated FeastRequest for postId: {}", agg.postId);
                        return feastRequest;
                    }
                })
                .name("Aggregation to FeastRequest");

        // 第七步：创建Feast Sink
        feastRequestStream.addSink(new FeastSinkFunction("Item Feature 1h Job"))
                .name("Feast Online Store Sink");

        // 第八步：创建sink，Redis环境
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(600);
        RedisUtil.addKvRocksSink(
                dataStream,
                redisConfig,
                true, // 异步写入
                100 // 批量大小
        );

        // 执行任务
        env.execute("Item Feature 1h Job");
    }

    /**
     * Item特征1小时聚合器
     */
    public static class ItemFeature1hAggregator
            implements AggregateFunction<UserFeatureEvent, ItemFeatureAccumulator, ItemFeature1hAggregation> {
        @Override
        public ItemFeatureAccumulator createAccumulator() {
            return new ItemFeatureAccumulator();
        }

        @Override
        public ItemFeatureAccumulator add(UserFeatureEvent event, ItemFeatureAccumulator accumulator) {
            return ItemFeatureCommon.addEventToAccumulator(event, accumulator);
        }

        @Override
        public ItemFeature1hAggregation getResult(ItemFeatureAccumulator accumulator) {
            ItemFeature1hAggregation result = new ItemFeature1hAggregation();
            result.postId = accumulator.postId;

            // 曝光特征
            result.postExpCnt1h = (int) accumulator.exposeHLL.cardinality();

            // 观看特征
            result.post3sviewCnt1h = (int) accumulator.view3sHLL.cardinality();
            result.post8sviewCnt1h = (int) accumulator.view8sHLL.cardinality();
            result.post12sviewCnt1h = (int) accumulator.view12sHLL.cardinality();
            result.post20sviewCnt1h = (int) accumulator.view20sHLL.cardinality();

            // 停留特征
            result.post5sstandCnt1h = (int) accumulator.stand5sHLL.cardinality();
            result.post10sstandCnt1h = (int) accumulator.stand10sHLL.cardinality();

            // 互动特征
            result.postLikeCnt1h = (int) accumulator.likeHLL.cardinality();
            result.postFollowCnt1h = (int) accumulator.followHLL.cardinality();
            result.postProfileCnt1h = (int) accumulator.profileHLL.cardinality();
            result.postPosinterCnt1h = (int) accumulator.posinterHLL.cardinality();

            result.updateTime = System.currentTimeMillis(); // 设置聚合结果的更新时间
            return result;
        }

        @Override
        public ItemFeatureAccumulator merge(ItemFeatureAccumulator a, ItemFeatureAccumulator b) {
            return ItemFeatureCommon.mergeAccumulators(a, b);
        }
    }

    /**
     * Item特征1小时聚合结果
     */
    public static class ItemFeature1hAggregation {
        public long postId;

        public long updateTime; // 新增：聚合结果的更新时间
        // 1小时曝光特征
        public int postExpCnt1h;

        // 1小时观看特征
        public int post3sviewCnt1h;
        public int post8sviewCnt1h;
        public int post12sviewCnt1h;
        public int post20sviewCnt1h;

        // 1小时停留特征
        public int post5sstandCnt1h;
        public int post10sstandCnt1h;

        // 1小时互动特征
        public int postLikeCnt1h;
        public int postFollowCnt1h;
        public int postProfileCnt1h;
        public int postPosinterCnt1h;
    }
}