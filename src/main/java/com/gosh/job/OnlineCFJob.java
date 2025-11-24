package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.entity.RecFeature;
import com.gosh.job.UserFeatureCommon.ExposeEventParser;
import com.gosh.job.UserFeatureCommon.ExposeToFeatureMapper;
import com.gosh.job.UserFeatureCommon.UserFeatureEvent;
import com.gosh.job.UserFeatureCommon.ViewEventParser;
import com.gosh.job.UserFeatureCommon.ViewToFeatureMapper;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Online Collaborative Filtering recall job based on Flink streaming.
 *
 * <p>Pipeline overview:
 * <ul>
 *     <li>Consume real-time user action logs from Kafka;</li>
 *     <li>Aggregate the actions inside 5-minute session windows by (user, recToken, item);</li>
 *     <li>Fetch the latest left-item history from Redis and build (left,right) co-occurrence pairs;</li>
 *     <li>Merge the incremental score with historical scores using time decay and persist back to Redis;</li>
 *     <li>Maintain a per-left-item Top-K right-item list for recall usage.</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class OnlineCFJob {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineCFJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        OnlineCFConfig config = new OnlineCFConfig();
        RedisConfig baseRedisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        RedisConfig historyRedisConfig = baseRedisConfig.copy();
        RedisConfig pairRedisConfig = baseRedisConfig.copy();
        pairRedisConfig.setTtl(config.getPairExpireSeconds());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setParallelism(config.getParallelism());

        java.util.Properties kafkaProperties = KafkaEnvUtil.loadProperties();
        kafkaProperties.setProperty("group.id", config.getKafkaGroupId());
        KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                kafkaProperties,
                "post");

        DataStream<String> kafkaStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "onlinecf-kafka-source");

        WatermarkStrategy<UserFeatureEvent> featureWatermark =
                WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserFeatureEvent>) (event, recordTimestamp) -> event.getTimestamp());

        DataStream<UserFeatureEvent> exposeFeatureStream = kafkaStream
                .flatMap(new ExposeEventParser())
                .name("parse-expose-events")
                .flatMap(new ExposeToFeatureMapper())
                .name("expose-to-feature");

        DataStream<UserFeatureEvent> viewFeatureStream = kafkaStream
                .flatMap(new ViewEventParser())
                .name("parse-view-events")
                .flatMap(new ViewToFeatureMapper())
                .name("view-to-feature");

        DataStream<UserFeatureEvent> featureStream = exposeFeatureStream
                .union(viewFeatureStream)
                .assignTimestampsAndWatermarks(featureWatermark)
                .name("unified-feature-stream");

        WatermarkStrategy<UserInteractionEvent> interactionWatermark =
                WatermarkStrategy.<UserInteractionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserInteractionEvent>) (event, recordTimestamp) -> event.timestamp);

        DataStream<UserInteractionEvent> interactionStream = featureStream
                .map(new FeatureEventToInteractionMapper())
                .name("feature-to-interaction")
                .assignTimestampsAndWatermarks(interactionWatermark)
                .filter((FilterFunction<UserInteractionEvent>) event -> event.uid > 0 && event.itemId > 0);

        DataStream<SessionInteractionScore> sessionScoreStream = interactionStream
                .keyBy((KeySelector<UserInteractionEvent, String>) UserInteractionEvent::sessionKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(config.getSessionWindowMinutes())))
                .aggregate(new SessionAggregateFunction(config), new SessionWindowProcessFunction())
                .name("session-score-aggregate")
                .filter((FilterFunction<SessionInteractionScore>) session -> session.totalScore > 0);

        DataStream<PairScoreEvent> pairCandidateStream = sessionScoreStream
                .flatMap(new UserHistoryJoinFunction(config, historyRedisConfig))
                .name("join-user-history")
                .filter((FilterFunction<PairScoreEvent>) pair -> pair.leftItemId > 0 && pair.rightItemId > 0);

        DataStream<PairScoreEvent> pairWindowedStream = pairCandidateStream
                .keyBy((KeySelector<PairScoreEvent, String>) PairScoreEvent::pairKey)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(config.getPairWindowMinutes())))
                .reduce(new PairScoreReducer(), new PairWindowProcessFunction())
                .name("pair-window-aggregate")
                .filter((FilterFunction<PairScoreEvent>) pair -> pair.score >= config.getPairMinScore());

        DataStream<PairScoreEvent> pairWithDecayStream = pairWindowedStream
                .flatMap(new PastPairCountDecayRedisFunction(config, pairRedisConfig))
                .name("pair-decay-merge")
                .filter((FilterFunction<PairScoreEvent>) pair -> pair.score >= config.getPairMinScore());

        // 写入 pair 计数（用于时间衰减）
        pairWithDecayStream.addSink(new PairCountRedisSink(config, pairRedisConfig))
                .name("redis-pair-count-sink");

        // 按 leftItem 聚合所有 rightItem，然后批量写入 index
        DataStream<Tuple2<Long, List<Tuple2<Long, Long>>>> leftItemAggregatedStream = pairWithDecayStream
                .map((MapFunction<PairScoreEvent, Tuple2<Long, List<Tuple2<Long, Long>>>>) pair ->
                        Tuple2.of(pair.leftItemId, Collections.singletonList(Tuple2.of(pair.rightItemId, pair.score))))
                .returns(Types.TUPLE(Types.LONG, Types.LIST(Types.TUPLE(Types.LONG, Types.LONG))))
                .keyBy((KeySelector<Tuple2<Long, List<Tuple2<Long, Long>>>, Long>) value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(config.getPairWindowMinutes())))
                .reduce((ReduceFunction<Tuple2<Long, List<Tuple2<Long, Long>>>>) (a, b) -> {
                    a.f1.addAll(b.f1);
                    return a;
                })
                .name("left-item-aggregate");

        leftItemAggregatedStream.addSink(new IndexTopKRedisSink(config, pairRedisConfig))
                .name("redis-index-topk-sink");

        env.execute("gosh-onlinecf");
    }

    // ------------------------------------------------------------------------
    // Data model
    // ------------------------------------------------------------------------

    private static class UserInteractionEvent {
        long uid;
        long itemId;
        String recToken;
        long timestamp;
        boolean expose;
        boolean view;
        boolean click;
        boolean longView;
        boolean like;
        boolean follow;
        boolean profileEnter;
        boolean favorite;
        boolean comment;
        boolean forward;
        boolean share;
        boolean download;

        String sessionKey() {
            return uid + "|" + recToken + "|" + itemId;
        }
    }

    private static class SessionAccumulator {
        long uid;
        long itemId;
        String recToken;
        long score;
        long latestTs;
    }

    private static class SessionInteractionScore {
        long uid;
        long rightItemId;
        long totalScore;
        long windowEndTs;
        String recToken;
    }

    private static class PairScoreEvent {
        long leftItemId;
        long rightItemId;
        long score;
        long eventTimestamp;

        PairScoreEvent withScore(long newScore) {
            this.score = newScore;
            return this;
        }

        String pairKey() {
            return leftItemId + "_" + rightItemId;
        }
    }

    private static class ItemHistory {
        long itemId;
        long timestamp;
        double weight;
    }

    // ------------------------------------------------------------------------
    // Parser & Aggregators
    // ------------------------------------------------------------------------

    private static class FeatureEventToInteractionMapper implements MapFunction<UserFeatureEvent, UserInteractionEvent> {

        @Override
        public UserInteractionEvent map(UserFeatureEvent event) {
            UserInteractionEvent interaction = new UserInteractionEvent();
            if (event == null) {
                return interaction;
            }
            interaction.uid = event.uid;
            interaction.itemId = event.postId;
            interaction.recToken = StringUtils.defaultString(event.recToken, "");
            interaction.timestamp = event.timestamp == 0 ? System.currentTimeMillis() : event.timestamp;
            interaction.expose = "expose".equalsIgnoreCase(event.eventType);
            interaction.view = "view".equalsIgnoreCase(event.eventType);
            interaction.click = interaction.view;
            interaction.longView = event.progressTime >= 3.0f || event.standingTime >= 5.0f;
            if (event.interaction != null) {
                for (Integer interactionType : event.interaction) {
                    if (interactionType == null) {
                        continue;
                    }
                    switch (interactionType) {
                        case 1:
                            interaction.like = true;
                            break;
                        case 13:
                            interaction.follow = true;
                            break;
                        case 15:
                            interaction.profileEnter = true;
                            break;
                        case 3:
                            interaction.comment = true;
                            break;
                        case 5:
                            interaction.favorite = true;
                            break;
                        case 6:
                            interaction.share = true;
                            break;
                        default:
                            break;
                    }
                }
            }
            return interaction;
        }
    }

    private static class SessionAggregateFunction implements AggregateFunction<UserInteractionEvent, SessionAccumulator, SessionAccumulator> {

        private final OnlineCFConfig config;

        SessionAggregateFunction(OnlineCFConfig config) {
            this.config = config;
        }

        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(UserInteractionEvent value, SessionAccumulator acc) {
            acc.uid = value.uid;
            acc.itemId = value.itemId;
            acc.recToken = value.recToken;
            acc.latestTs = Math.max(acc.latestTs, value.timestamp);
            acc.score += computeScore(value);
            return acc;
        }

        @Override
        public SessionAccumulator getResult(SessionAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            SessionAccumulator merged = new SessionAccumulator();
            merged.uid = a.uid == 0 ? b.uid : a.uid;
            merged.itemId = a.itemId == 0 ? b.itemId : a.itemId;
            merged.recToken = StringUtils.defaultIfBlank(a.recToken, b.recToken);
            merged.score = a.score + b.score;
            merged.latestTs = Math.max(a.latestTs, b.latestTs);
            return merged;
        }

        private long computeScore(UserInteractionEvent event) {
            long score = 0;
            if (event.click) {
                score += config.getActionWeight("click");
            }
            if (event.longView) {
                score += config.getActionWeight("long_view");
            }
            if (event.like) {
                score += config.getActionWeight("like");
            }
            if (event.follow) {
                score += config.getActionWeight("follow");
            }
            if (event.profileEnter) {
                score += config.getActionWeight("profile_enter");
            }
            if (event.favorite) {
                score += config.getActionWeight("favorite");
            }
            if (event.comment) {
                score += config.getActionWeight("comment");
            }
            if (event.forward) {
                score += config.getActionWeight("forward");
            }
            if (event.share) {
                score += config.getActionWeight("share");
            }
            if (event.download) {
                score += config.getActionWeight("download");
            }
            return score;
        }
    }

    private static class SessionWindowProcessFunction extends ProcessWindowFunction<SessionAccumulator, SessionInteractionScore, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<SessionAccumulator> elements,
                            Collector<SessionInteractionScore> out) {
            SessionAccumulator acc = elements.iterator().next();
            if (acc == null || acc.uid <= 0 || acc.itemId <= 0 || acc.score <= 0) {
                return;
            }
            SessionInteractionScore score = new SessionInteractionScore();
            score.uid = acc.uid;
            score.rightItemId = acc.itemId;
            score.recToken = acc.recToken;
            score.totalScore = acc.score;
            score.windowEndTs = context.window().getEnd();
            out.collect(score);
        }
    }

    private static class PairScoreReducer implements ReduceFunction<PairScoreEvent> {
        @Override
        public PairScoreEvent reduce(PairScoreEvent value1, PairScoreEvent value2) {
            value1.score += value2.score;
            value1.eventTimestamp = Math.max(value1.eventTimestamp, value2.eventTimestamp);
            return value1;
        }
    }

    private static class PairWindowProcessFunction extends ProcessWindowFunction<PairScoreEvent, PairScoreEvent, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<PairScoreEvent> elements,
                            Collector<PairScoreEvent> out) {
            PairScoreEvent event = elements.iterator().next();
            if (event != null) {
                event.eventTimestamp = context.window().getEnd();
                out.collect(event);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Redis interactions
    // ------------------------------------------------------------------------

    private static class UserHistoryJoinFunction extends RichFlatMapFunction<SessionInteractionScore, PairScoreEvent> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private final UserHistoryParser parser;
        private transient RedisHelper redisHelper;

        public UserHistoryJoinFunction(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
            this.parser = new UserHistoryParser(config);
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
        }

        @Override
        public void flatMap(SessionInteractionScore value,
                            Collector<PairScoreEvent> out) throws Exception {
            if (value == null || value.uid <= 0) {
                return;
            }
            String redisKey = config.getHistoryKeyPrefix() + value.uid + config.getHistoryKeySuffix();
            byte[] raw = redisHelper.getValue(redisKey);
            if (raw == null || raw.length == 0) {
                return;
            }
            List<ItemHistory> histories = parser.parse(raw);
            if (histories.isEmpty()) {
                return;
            }
            for (ItemHistory history : histories) {
                if (history.itemId <= 0) {
                    continue;
                }
                PairScoreEvent pair = new PairScoreEvent();
                pair.leftItemId = history.itemId;
                pair.rightItemId = value.rightItemId;
                long scaledScore = Math.round(value.totalScore * history.weight);
                pair.score = Math.max(1, scaledScore);
                pair.eventTimestamp = value.windowEndTs;
                out.collect(pair);
            }
        }

        @Override
        public void close() {
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    private static class PastPairCountDecayRedisFunction extends RichFlatMapFunction<PairScoreEvent, PairScoreEvent> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private transient RedisHelper redisHelper;

        PastPairCountDecayRedisFunction(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
        }

        @Override
        public void flatMap(PairScoreEvent value, Collector<PairScoreEvent> out) {
            if (value == null) {
                return;
            }
            String key = config.getPairKeyPrefix() + value.leftItemId + "_" + value.rightItemId;
            String cached = redisHelper.getStringValue(key);
            long currentMinute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            long decayed = countDecay(cached, config.getDecayWeight(), config.getDecayStepMinutes(), currentMinute);
            value.score += decayed;
            out.collect(value);
        }

        @Override
        public void close() {
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    /**
     * 写入 pair 计数到 Redis（用于时间衰减计算）
     */
    private static class PairCountRedisSink extends RichSinkFunction<PairScoreEvent> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private transient RedisHelper redisHelper;

        PairCountRedisSink(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
        }

        @Override
        public void invoke(PairScoreEvent value, Context context) {
            if (value == null || value.leftItemId <= 0 || value.rightItemId <= 0) {
                return;
            }
            long currentMinute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            String pairKey = config.getPairKeyPrefix() + value.leftItemId + "_" + value.rightItemId;
            String pairValue = value.score + ":" + currentMinute;
            redisHelper.setex(pairKey, config.getPairExpireSeconds(), pairValue);
        }

        @Override
        public void close() {
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    /**
     * 批量写入 index Top-K 到 Redis ZSet
     * 读取现有 Top-K，合并新数据，排序后重新写入
     */
    private static class IndexTopKRedisSink extends RichSinkFunction<Tuple2<Long, List<Tuple2<Long, Long>>>> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private transient RedisHelper redisHelper;

        IndexTopKRedisSink(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
        }

        @Override
        public void invoke(Tuple2<Long, List<Tuple2<Long, Long>>> value, Context context) {
            if (value == null || value.f0 == null || value.f1 == null || value.f1.isEmpty()) {
                return;
            }
            long leftItemId = value.f0;
            String indexKey = config.getIndexKeyPrefix() + leftItemId;

            // 1. 读取现有的 Top-K（从高到低），用于合并历史数据
            // 原版逻辑：读取现有 index，排除新数据中已存在的 rightItem，避免重复计算
            List<Long> newRightItemIds = value.f1.stream()
                    .map(pair -> pair.f0)
                    .collect(Collectors.toList());

            List<String> existingTopK = redisHelper.getTopK(indexKey, config.getIndexLimit());
            List<Tuple2<Long, Long>> existingPairs = new ArrayList<>();
            for (String member : existingTopK) {
                try {
                    long rightItemId = Long.parseLong(member);
                    // 原版逻辑：排除新数据中已存在的 rightItem（新数据会覆盖）
                    if (!newRightItemIds.contains(rightItemId)) {
                        Double score = redisHelper.zscore(indexKey, member);
                        if (score != null) {
                            existingPairs.add(Tuple2.of(rightItemId, score.longValue()));
                        }
                    }
                } catch (NumberFormatException e) {
                    // 忽略无效的 member
                }
            }

            // 2. 合并新数据和历史数据
            List<Tuple2<Long, Long>> allPairs = new ArrayList<>(value.f1);
            allPairs.addAll(existingPairs);

            // 3. 按 score 从高到低排序，去重并保留最高分
            Map<Long, Long> scoreMap = new HashMap<>();
            for (Tuple2<Long, Long> pair : allPairs) {
                long rightItemId = pair.f0;
                long score = pair.f1;
                scoreMap.merge(rightItemId, score, Math::max);
            }

            // 4. 按 score 从高到低排序，过滤并限制 Top-K
            List<Map.Entry<Long, Long>> sorted = scoreMap.entrySet().stream()
                    .filter(entry -> entry.getValue() >= config.getIndexMinScore())
                    .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                    .limit(config.getIndexLimit())
                    .collect(Collectors.toList());

            // 5. 删除旧的 ZSet，重新写入 Top-K（原版也是覆盖整个 index）
            redisHelper.del(indexKey);

            // 6. 批量写入 ZSet（按 score 从高到低）
            for (Map.Entry<Long, Long> entry : sorted) {
                redisHelper.zadd(indexKey, entry.getValue().doubleValue(), String.valueOf(entry.getKey()));
            }

            // 7. 设置过期时间
            if (!sorted.isEmpty()) {
                redisHelper.expire(indexKey, config.getIndexExpireSeconds());
            }
        }

        @Override
        public void close() {
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    // ------------------------------------------------------------------------
    // Helper classes
    // ------------------------------------------------------------------------

    private static class UserHistoryParser {

        private final OnlineCFConfig config;

        UserHistoryParser(OnlineCFConfig config) {
            this.config = config;
        }

        List<ItemHistory> parse(byte[] payload) {
            // First try to parse protobuf RecUserFeature
            try {
                RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(payload);
                List<ItemHistory> result = new ArrayList<>();
                mergeHistory(result, feature.getViewer3SviewPostHis24H(), 1.0);
                mergeHistory(result, feature.getViewer5SstandPostHis24H(), 1.0);
                mergeHistory(result, feature.getViewerLikePostHis24H(), 1.2);
                mergeHistory(result, feature.getViewerFollowPostHis24H(), 1.3);
                mergeHistory(result, feature.getViewerProfilePostHis24H(), 0.8);
                mergeHistory(result, feature.getViewerPosinterPostHis24H(), 1.1);
                return limitAndFilter(result);
            } catch (Exception protoError) {
                String raw = new String(payload, StandardCharsets.UTF_8);
                return parsePlainString(raw);
            }
        }

        private void mergeHistory(List<ItemHistory> target, String raw, double baseWeight) {
            if (StringUtils.isBlank(raw)) {
                return;
            }
            String[] tokens = raw.split(",");
            for (String token : tokens) {
                String trimmed = token.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                String[] parts = trimmed.split("\\|");
                long itemId = NumberUtils.toLong(parts[0], 0L);
                if (itemId <= 0) {
                    continue;
                }
                long ts = System.currentTimeMillis();
                if (parts.length > 1) {
                    long candidate = NumberUtils.toLong(parts[1], 0L);
                    if (candidate > 1000000L) {
                        ts = TimeUnit.SECONDS.toMillis(candidate);
                    }
                }
                ItemHistory history = new ItemHistory();
                history.itemId = itemId;
                history.timestamp = ts;
                history.weight = baseWeight;
                target.add(history);
            }
        }

        private List<ItemHistory> parsePlainString(String raw) {
            List<ItemHistory> result = new ArrayList<>();
            if (StringUtils.isBlank(raw)) {
                return result;
            }
            try {
                JsonNode arrayNode = OBJECT_MAPPER.readTree(raw);
                if (arrayNode.isArray()) {
                    for (JsonNode node : arrayNode) {
                        long itemId = node.path("itemId").asLong(node.path("item_id").asLong(0));
                        if (itemId <= 0) {
                            continue;
                        }
                        long ts = node.path("ts").asLong(System.currentTimeMillis());
                        ItemHistory history = new ItemHistory();
                        history.itemId = itemId;
                        history.timestamp = ts;
                        history.weight = node.path("weight").asDouble(1.0);
                        result.add(history);
                    }
                    return limitAndFilter(result);
                }
            } catch (IOException ignore) {
                // fall back to simple parsing
            }
            mergeHistory(result, raw, 1.0);
            return limitAndFilter(result);
        }

        private List<ItemHistory> limitAndFilter(List<ItemHistory> input) {
            long expireMillis = TimeUnit.HOURS.toMillis(config.getHistoryMaxAgeHours());
            long now = System.currentTimeMillis();
            List<ItemHistory> filtered = input.stream()
                    .filter(history -> history.itemId > 0)
                    .filter(history -> expireMillis <= 0 || now - history.timestamp <= expireMillis)
                    .sorted(Comparator.comparingLong((ItemHistory h) -> h.timestamp).reversed())
                    .collect(Collectors.toList());
            LinkedHashMap<Long, ItemHistory> dedup = new LinkedHashMap<>();
            for (ItemHistory history : filtered) {
                dedup.putIfAbsent(history.itemId, history);
            }
            return dedup.values().stream().limit(config.getHistoryMaxItems()).collect(Collectors.toList());
        }
    }

    private static long countDecay(String storedValue, double decayWeight, int decayStepMinutes, long currentMinuteTs) {
        if (StringUtils.isBlank(storedValue)) {
            return 0L;
        }
        String[] parts = storedValue.split(":");
        if (parts.length < 2) {
            return 0L;
        }
        long cachedScore = NumberUtils.toLong(parts[0], 0L);
        long cachedMinute = NumberUtils.toLong(parts[1], 0L);
        long steps = Math.max(0L, currentMinuteTs - cachedMinute);
        double stepSize = decayStepMinutes <= 0 ? 1.0 : decayStepMinutes;
        double factor = Math.pow(decayWeight, steps / stepSize);
        return Math.round(cachedScore * factor);
    }

    private static class RedisHelper {
        private final RedisConfig redisConfig;
        private transient RedisConnectionManager connectionManager;
        private transient RedisCommands<String, Tuple2<String, byte[]>> commands;
        private transient RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> clusterCommands;
        private boolean clusterMode;

        RedisHelper(RedisConfig redisConfig) {
            this.redisConfig = redisConfig;
        }

        void open() {
            this.connectionManager = RedisConnectionManager.getInstance(redisConfig);
            this.clusterMode = redisConfig.isClusterMode();
            if (clusterMode) {
                this.clusterCommands = connectionManager.getRedisClusterCommands();
            } else {
                this.commands = connectionManager.getRedisCommands();
            }
        }

        byte[] getValue(String key) {
            Tuple2<String, byte[]> result = clusterMode
                    ? clusterCommands.get(key)
                    : commands.get(key);
            return result == null ? null : result.f1;
        }

        String getStringValue(String key) {
            byte[] value = getValue(key);
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        }

        void setex(String key, int ttlSeconds, String value) {
            Tuple2<String, byte[]> tuple = wrap(value);
            if (ttlSeconds > 0) {
                if (clusterMode) {
                    clusterCommands.setex(key, ttlSeconds, tuple);
                } else {
                    commands.setex(key, ttlSeconds, tuple);
                }
            } else {
                if (clusterMode) {
                    clusterCommands.set(key, tuple);
                } else {
                    commands.set(key, tuple);
                }
            }
        }

        /**
         * 向 ZSet 添加或更新元素
         * Redis ZADD 默认行为：如果 member 已存在，则更新其 score
         * ZSet 内部按 score 升序存储（从低到高）
         */
        void zadd(String key, double score, String member) {
            Tuple2<String, byte[]> tuple = wrap(member);
            if (clusterMode) {
                clusterCommands.zadd(key, score, tuple);
            } else {
                commands.zadd(key, score, tuple);
            }
        }

        /**
         * 获取 ZSet 的元素数量
         */
        long zcard(String key) {
            return clusterMode ? clusterCommands.zcard(key) : commands.zcard(key);
        }

        /**
         * 删除 ZSet 中指定排名范围的元素（按升序排名）
         * Redis ZSet 默认按 score 升序，rank 0 是最低分
         * 删除 rank 0 到 end 的元素 = 删除最低分的元素，保留最高分
         *
         * @param key ZSet key
         * @param start 起始排名（包含，0 表示最低分）
         * @param end 结束排名（包含）
         */
        void zremRangeByRank(String key, long start, long end) {
            if (clusterMode) {
                clusterCommands.zremrangebyrank(key, start, end);
            } else {
                commands.zremrangebyrank(key, start, end);
            }
        }

        /**
         * 获取 ZSet 中从高到低排序的元素（降序，按 score 从大到小）
         * Redis ZSet 默认是升序，所以删除时删除最低分（rank 0），保留最高分
         * 读取时使用 ZREVRANGE 获取从高到低的顺序
         *
         * @param key ZSet key
         * @param start 起始排名（0 表示最高分）
         * @param end 结束排名（-1 表示最低分）
         * @return 按 score 从高到低排序的 member 列表
         */
        List<String> zrevrange(String key, long start, long end) {
            List<Tuple2<String, byte[]>> result;
            if (clusterMode) {
                result = clusterCommands.zrevrange(key, start, end);
            } else {
                result = commands.zrevrange(key, start, end);
            }
            return result.stream()
                    .map(tuple -> new String(tuple.f1, StandardCharsets.UTF_8))
                    .collect(Collectors.toList());
        }

        /**
         * 获取 ZSet 中指定 member 的 score
         *
         * @param key ZSet key
         * @param member member 值
         * @return score，如果 member 不存在返回 null
         */
        Double zscore(String key, String member) {
            Tuple2<String, byte[]> tuple = wrap(member);
            return clusterMode ? clusterCommands.zscore(key, tuple) : commands.zscore(key, tuple);
        }

        /**
         * 获取 Top-K 元素（从高到低）
         *
         * @param key ZSet key
         * @param limit 返回的元素数量
         * @return 按 score 从高到低排序的 Top-K member 列表
         */
        List<String> getTopK(String key, int limit) {
            return zrevrange(key, 0, limit - 1);
        }

        void expire(String key, int ttlSeconds) {
            if (ttlSeconds <= 0) {
                return;
            }
            if (clusterMode) {
                clusterCommands.expire(key, ttlSeconds);
            } else {
                commands.expire(key, ttlSeconds);
            }
        }

        /**
         * 删除指定的 key
         */
        void del(String key) {
            if (clusterMode) {
                clusterCommands.del(key);
            } else {
                commands.del(key);
            }
        }

        void close() {
            if (connectionManager != null) {
                connectionManager.shutdown();
            }
        }

        private Tuple2<String, byte[]> wrap(String value) {
            return new Tuple2<>("", value.getBytes(StandardCharsets.UTF_8));
        }
    }

    // ------------------------------------------------------------------------
    // Configuration - All config values are hardcoded constants
    // ------------------------------------------------------------------------

    static class OnlineCFConfig {
        // Kafka配置
        private static final String KAFKA_GROUP_ID = "gosh-onlinecf";

        // 窗口配置
        private static final int SESSION_WINDOW_MINUTES = 5;
        private static final int PAIR_WINDOW_MINUTES = 2;

        // 衰减配置
        private static final double DECAY_WEIGHT = 0.96;
        private static final int DECAY_STEP_MINUTES = 2;

        // Pair配置
        private static final long PAIR_MIN_SCORE = 25L;
        private static final int PAIR_EXPIRE_SECONDS = 12 * 3600; // 12小时

        // Index配置
        private static final int INDEX_LIMIT = 128;
        private static final int INDEX_EXPIRE_SECONDS = 4 * 3600; // 4小时
        private static final long INDEX_MIN_SCORE = 25L;

        // Redis Key配置
        private static final String HISTORY_KEY_PREFIX = "rec:user_feature:{";
        private static final String HISTORY_KEY_SUFFIX = "}:post24h";
        private static final String PAIR_KEY_PREFIX = "roc:pair:";
        private static final String INDEX_KEY_PREFIX = "roc:index:";

        // 历史记录配置
        private static final int HISTORY_MAX_ITEMS = 50;
        private static final long HISTORY_MAX_AGE_HOURS = 24L;

        // Action权重配置
        private static final int ACTION_WEIGHT_CLICK = 1;
        private static final int ACTION_WEIGHT_LONG_VIEW = 6;
        private static final int ACTION_WEIGHT_LIKE = 4;
        private static final int ACTION_WEIGHT_FOLLOW = 8;
        private static final int ACTION_WEIGHT_PROFILE_ENTER = 4;
        private static final int ACTION_WEIGHT_FAVORITE = 4;
        private static final int ACTION_WEIGHT_COMMENT = 4;
        private static final int ACTION_WEIGHT_FORWARD = 8;
        private static final int ACTION_WEIGHT_SHARE = 8;
        private static final int ACTION_WEIGHT_DOWNLOAD = 5;

        // Flink配置
        private static final int PARALLELISM = 4;

        String getKafkaGroupId() {
            return KAFKA_GROUP_ID;
        }

        int getSessionWindowMinutes() {
            return SESSION_WINDOW_MINUTES;
        }

        int getPairWindowMinutes() {
            return PAIR_WINDOW_MINUTES;
        }

        double getDecayWeight() {
            return DECAY_WEIGHT;
        }

        int getDecayStepMinutes() {
            return DECAY_STEP_MINUTES;
        }

        long getPairMinScore() {
            return PAIR_MIN_SCORE;
        }

        int getPairExpireSeconds() {
            return PAIR_EXPIRE_SECONDS;
        }

        int getIndexLimit() {
            return INDEX_LIMIT;
        }

        int getIndexExpireSeconds() {
            return INDEX_EXPIRE_SECONDS;
        }

        long getIndexMinScore() {
            return INDEX_MIN_SCORE;
        }

        String getHistoryKeyPrefix() {
            return HISTORY_KEY_PREFIX;
        }

        String getHistoryKeySuffix() {
            return HISTORY_KEY_SUFFIX;
        }

        int getHistoryMaxItems() {
            return HISTORY_MAX_ITEMS;
        }

        long getHistoryMaxAgeHours() {
            return HISTORY_MAX_AGE_HOURS;
        }

        String getPairKeyPrefix() {
            return PAIR_KEY_PREFIX;
        }

        String getIndexKeyPrefix() {
            return INDEX_KEY_PREFIX;
        }

        int getActionWeight(String action) {
            switch (action) {
                case "click":
                    return ACTION_WEIGHT_CLICK;
                case "long_view":
                    return ACTION_WEIGHT_LONG_VIEW;
                case "like":
                    return ACTION_WEIGHT_LIKE;
                case "follow":
                    return ACTION_WEIGHT_FOLLOW;
                case "profile_enter":
                    return ACTION_WEIGHT_PROFILE_ENTER;
                case "favorite":
                    return ACTION_WEIGHT_FAVORITE;
                case "comment":
                    return ACTION_WEIGHT_COMMENT;
                case "forward":
                    return ACTION_WEIGHT_FORWARD;
                case "share":
                    return ACTION_WEIGHT_SHARE;
                case "download":
                    return ACTION_WEIGHT_DOWNLOAD;
                default:
                    return 1;
            }
        }

        int getParallelism() {
            return PARALLELISM;
        }
    }
}

