package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.state.*;
import com.gosh.job.UserFeatureCommon.*;
import org.apache.flink.api.common.state.*;

public class UserPornLabelJob {
    private static final Logger LOG = LoggerFactory.getLogger(UserPornLabelJob.class);

    // Redis Key 前缀和后缀
    private static final String RedisKey = "rec_post:{%d}:rtylevel";

    private static final int REDIS_TTL = 3 * 60;

    // Kafka Group ID
    private static final String KAFKA_GROUP_ID = "rec_porn_label";
    private static long testUid = 117134;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Flink 任务启动(UserPornLabelJob) ===");
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
                    .name("Pre-filter View Events");

            // 5. 解析观看事件
            System.out.println("5. 解析观看事件...");
            SingleOutputStreamOperator<PostViewEvent> viewStream = filteredStream
                    .flatMap(new ViewEventParser())
                    .name("Parse View Events");

            // 6.保留最近 10 条记录
            SingleOutputStreamOperator<UserNExposures> recentStats = viewStream
                    .keyBy(event -> event.uid)
                    .process(new RecentNExposures())
                    .map(event -> {
                        if (event.viewer == testUid) {
                            System.out.println("[UserNExposures] UID: " + event.viewer + event.toString());
                        }
                        return event;
                    })
                    .name("recent-exposure-statistics");
            //7. 计算 用户 属于的色情群体
            List<Integer> positiveActions = Arrays.asList(1,3,5,6); // 点赞，评论，分享，收藏
            DataStream<Tuple2<String, byte[]>> dataStream =recentStats
                    .map(new MapFunction<UserNExposures, Tuple2<String, byte[]>>() {
                        @Override
                        public Tuple2<String, byte[]> map(UserNExposures event) throws Exception {
                            String pornLabel = getPornLabel(event, positiveActions);
                            if (event.viewer == testUid) {
                                System.out.println("[---redis val] UID: " + event.viewer + pornLabel + event.toString());
                            }
                            // 构建 Redis key
                            String redisKey = String.format(RedisKey, event.viewer);
                            return new Tuple2<>(redisKey, pornLabel.getBytes());
                        }
                    })
                    .name("cal user porn label");

            // 8. 创建 Redis Sink
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
            env.execute("UserPornLabelJob");

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

    public static String getPornLabel(UserNExposures event,  List<Integer> positiveActions) {
        Map<String, Float> standingStatistics = new HashMap<>(); //pornLabel -> standingTime
        Map<String, Integer> positiveStatistics = new HashMap<>(); //pornLabel -> positiveCount
        Map<String, Integer> negativeStatistics = new HashMap<>(); //pornLabel -> positiveCount

        float allStandTime = 0.0f;
        for (Tuple4<List<PostViewInfo>, Long, Integer, String> tuple :event.firstNExposures) {
            String pornTag = tuple.f3;
            float standingTime = 0.0f;
            int positiveCount = 0;
            int negativeCount = 0;

            for (PostViewInfo info : tuple.f0) {
                if (info != null) {
                    standingTime += info.standingTime;
                    if (info.interaction != null && !info.interaction.isEmpty()) {
                        for (int action : info.interaction) {
                            if (positiveActions.contains(action)) {
                                positiveCount++;
                            } else if (action == 11) { // 不感兴趣
                                negativeCount++;
                            }
                        }
                    }
                }
            }
            allStandTime += standingTime;
            float stTime = standingStatistics.getOrDefault(pornTag, 0.0f);
            standingStatistics.put(pornTag, stTime + standingTime);
            int pCount = positiveStatistics.getOrDefault(pornTag, 0);
            positiveStatistics.put(pornTag, pCount + positiveCount);
            int negCount = negativeStatistics.getOrDefault(pornTag, 0);
            negativeStatistics.put(pornTag, negCount + negativeCount);
        }
        String pornLabel = "u_ylevel_unk";
        for (Map.Entry<String, Float> entry : standingStatistics.entrySet()) {
            if ( (entry.getValue() / allStandTime > 0.6 | positiveStatistics.get(entry.getKey()) > 0) &
                    negativeStatistics.get(entry.getKey()) <= 0 ) {
                pornLabel = "u_ylevel_"+ entry.getKey();
                break;
            }
        }
        return pornLabel;
    }

    // 前N次曝光统计输出
    public static class UserNExposures  implements Serializable {
        public long viewer;
        public List<Tuple4<List<PostViewInfo>, Long, Integer, String>>  firstNExposures;  // 前N次曝光记录
        public long collectionTime;  // 收集完成时间

        public UserNExposures(long viewer, List<Tuple4<List<PostViewInfo>, Long, Integer, String>> exposures) {
            this.viewer = viewer;
            if (exposures == null || exposures.isEmpty()) {
                this.firstNExposures = new ArrayList<>();
            } else {
                this.firstNExposures = exposures;
            }
            this.collectionTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return String.format("User %d collected %s exposures", viewer, firstNExposures.toString());
        }
    }

    static class RecentNExposures extends KeyedProcessFunction<Long, PostViewEvent, UserNExposures> {
        private static final int N = 10;  // 保留最近10次
        private Long LastTime = 0L;
        private transient ListState<Tuple4<List<PostViewInfo>, Long, Integer, String>> recentViewEventState;  //
        private transient RedisConnectionManager redisManager;

        @Override
        public void open(Configuration parameters) {
            redisManager = RedisConnectionManager.getInstance(RedisConfig.fromProperties(RedisUtil.loadProperties()));
            recentViewEventState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recentViewEvent",
                            org.apache.flink.api.common.typeinfo.Types.TUPLE(
                                    org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(PostViewInfo.class)),
                                    org.apache.flink.api.common.typeinfo.Types.LONG,
                                    org.apache.flink.api.common.typeinfo.Types.INT,
                                    org.apache.flink.api.common.typeinfo.Types.STRING
                            )
                    )
            );
        }
        @Override
        public void close() throws Exception {
            if (redisManager != null) {
                redisManager.shutdown();
            }
            super.close();
        }

        @Override
        public void processElement(PostViewEvent event, Context ctx, Collector<UserNExposures> out) throws Exception {

            long currentTime = ctx.timestamp();
            long viewerId = event.uid;

            // 获取当前所有记录
            List<Tuple4<List<PostViewInfo>, Long, Integer, String>> allRecords = new ArrayList<>();

            for (Tuple4<List<PostViewInfo>, Long, Integer, String> record : recentViewEventState.get()) {
                allRecords.add(record);
            }

            // 添加新记录，包含序号
            for (PostViewInfo info : event.infoList) { // 按 postid 归类
                boolean exist = false;
                for (Tuple4<List<PostViewInfo>, Long, Integer, String> record : allRecords) {
                    if (record.f1 == info.postId) { // 已存在
                        record.f0.add(info);
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    String pornTag = getPostTagFromRedis(info.postId).get();
                    List<PostViewInfo> infos = new ArrayList<>();
                    infos.add(info);
                    allRecords.add(new Tuple4<>(
                            infos,
                            info.postId,
                            allRecords.size() + 1,
                            pornTag
                    ));
                }
            }

            // 按 num 排序（如果需要）
            allRecords.sort(Comparator.comparing(r -> r.f2));

            // 只保留最近N条记录
            if (allRecords.size() > N) {
                allRecords = new ArrayList<>(allRecords.subList(allRecords.size() - N, allRecords.size()));
            }

            // 更新状态
            recentViewEventState.clear();
            for (Tuple4<List<PostViewInfo>, Long, Integer, String> record : allRecords) {
                recentViewEventState.add(record);
            }

            // 输出最新状态
            if (allRecords.size() == N) { //
                UserNExposures result = new UserNExposures(viewerId, allRecords);
                out.collect(result);
            }
        }
        String UNK = "unk";
        private CompletableFuture<String> getPostTagFromRedis(long postId) {
            String redisKey = "rec_post:{" + postId + "}:aitag";
            return redisManager.executeStringAsync(
                    commands -> {
                        try {
                            org.apache.flink.api.java.tuple.Tuple2<String, byte[]> tuple = commands.get(redisKey);
                            if (tuple != null && tuple.f1 != null && tuple.f1.length > 0) {
                                String value = new String(tuple.f1, java.nio.charset.StandardCharsets.UTF_8);
                                if (value.isEmpty()) {
                                    return UNK;
                                }
                                return selectPornTag(value);
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to fetch tag from Redis for postId {}: {}", postId, e.getMessage());
                        }
                        return UNK;
                    }
            );
        }
        private String selectPornTag(String rawValue) {
            String[] tags = rawValue.split(",");
            String contentCandidate = UNK;
            for (String tag : tags) {
                if (tag == null) {
                    continue;
                }
                String trimmed = tag.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.contains("restricted#")) {
                    String[] vals = trimmed.split("#");
                    if (vals.length == 2) {
                        return vals[1];
                    }
                    break;
                }
            }
            return contentCandidate;
        }

    }

}