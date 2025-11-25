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
 */
@SuppressWarnings("unused")
public class OnlineCFJobWithDetailedLogs {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineCFJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        System.out.println("=== OnlineCFJob 启动 ===");
        System.out.println("启动时间: " + new Date());
        System.out.println("参数: " + Arrays.toString(args));

        try {
            OnlineCFConfig config = new OnlineCFConfig();
            System.out.println("1. 加载配置完成");

            RedisConfig baseRedisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
            RedisConfig historyRedisConfig = baseRedisConfig.copy();
            RedisConfig pairRedisConfig = baseRedisConfig.copy();
            pairRedisConfig.setTtl(config.getPairExpireSeconds());
            System.out.println("2. Redis配置初始化完成");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.setParallelism(config.getParallelism());
            System.out.println("3. Flink环境配置完成，并行度: " + config.getParallelism());

            java.util.Properties kafkaProperties = KafkaEnvUtil.loadProperties();
            kafkaProperties.setProperty("group.id", config.getKafkaGroupId());
            System.out.println("4. Kafka配置: group.id=" + config.getKafkaGroupId());

            KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                    kafkaProperties,
                    "post");
            System.out.println("5. Kafka Source创建完成");

            DataStream<String> kafkaStream = env.fromSource(kafkaSource,
                    WatermarkStrategy.noWatermarks(), "onlinecf-kafka-source");
            System.out.println("6. Kafka数据流创建完成");

            // 添加Kafka消息调试
            DataStream<String> debugKafkaStream = kafkaStream
                    .map(value -> {
                        System.out.println("Kafka原始消息: " + (value.length() > 200 ? value.substring(0, 200) + "..." : value));
                        return value;
                    })
                    .name("debug-kafka-messages");

            WatermarkStrategy<UserFeatureEvent> featureWatermark =
                    WatermarkStrategy.<UserFeatureEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                            .withTimestampAssigner((SerializableTimestampAssigner<UserFeatureEvent>) (event, recordTimestamp) -> {
                                long ts = event.getTimestamp();
                                System.out.println("特征事件时间戳分配: " + ts + ", 事件类型: " + event.eventType);
                                return ts;
                            });

            System.out.println("7. 开始解析曝光事件...");
            DataStream<UserFeatureEvent> exposeFeatureStream = debugKafkaStream
                    .flatMap(new ExposeEventParser())
                    .name("parse-expose-events")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("解析曝光事件 - UID: " + event.uid + ", PostID: " + event.infoList.stream().map(e -> e.postId).collect(Collectors.toList()).toString());
                        }
                        return event;
                    })
                    .name("debug-expose-events")
                    .flatMap(new ExposeToFeatureMapper())
                    .name("expose-to-feature")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("曝光特征事件 - UID: " + event.uid + ", PostID: " + event.postId + ", 时间戳: " + event.timestamp);
                        }
                        return event;
                    })
                    .name("debug-expose-features");

            System.out.println("8. 开始解析观看事件...");
            DataStream<UserFeatureEvent> viewFeatureStream = debugKafkaStream
                    .flatMap(new ViewEventParser())
                    .name("parse-view-events")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("解析观看事件 - UID: " + event.uid + ", PostID: " + event.infoList.stream().map(e -> e.postId).collect(Collectors.toList()).toString()+ ", 进度: " + event.infoList.stream().map(e -> e.progressTime).collect(Collectors.toList()).toString());
                        }
                        return event;
                    })
                    .name("debug-view-events")
                    .flatMap(new ViewToFeatureMapper())
                    .name("view-to-feature")
                    .map(event -> {
                        if (event != null) {
                            System.out.println("观看特征事件 - UID: " + event.uid + ", PostID: " + event.postId + ", 时间戳: " + event.timestamp);
                        }
                        return event;
                    })
                    .name("debug-view-features");

            DataStream<UserFeatureEvent> featureStream = exposeFeatureStream
                    .union(viewFeatureStream)
                    .assignTimestampsAndWatermarks(featureWatermark)
                    .name("unified-feature-stream")
                    .map(event -> {
                        System.out.println("统一特征流事件 - UID: " + event.uid + ", PostID: " + event.postId + ", 类型: " + event.eventType);
                        return event;
                    })
                    .name("debug-unified-features");

            System.out.println("9. 开始转换交互事件...");
            WatermarkStrategy<UserInteractionEvent> interactionWatermark =
                    WatermarkStrategy.<UserInteractionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((SerializableTimestampAssigner<UserInteractionEvent>) (event, recordTimestamp) -> {
                                System.out.println("交互事件时间戳: " + event.timestamp);
                                return event.timestamp;
                            });

            DataStream<UserInteractionEvent> interactionStream = featureStream
                    .map(new FeatureEventToInteractionMapper())
                    .name("feature-to-interaction")
                    .map(event -> {
                        System.out.println("转换后交互事件 - UID: " + event.uid + ", ItemID: " + event.itemId +
                                ", SessionKey: " + event.sessionKey() + ", 时间戳: " + event.timestamp);
                        return event;
                    })
                    .name("debug-interaction-events")
                    .assignTimestampsAndWatermarks(interactionWatermark)
                    .filter((FilterFunction<UserInteractionEvent>) event -> {
                        boolean valid = event.uid > 0 && event.itemId > 0;
                        if (!valid) {
                            System.out.println("过滤无效交互事件 - UID: " + event.uid + ", ItemID: " + event.itemId);
                        }
                        return valid;
                    });

            System.out.println("10. 开始会话窗口聚合...");
            DataStream<SessionInteractionScore> sessionScoreStream = interactionStream
                    .keyBy((KeySelector<UserInteractionEvent, String>) event -> {
                        String key = event.sessionKey();
                        System.out.println("KeyBy Session: " + key);
                        return key;
                    })
                    .window(TumblingEventTimeWindows.of(Time.minutes(config.getSessionWindowMinutes())))
                    .aggregate(new SessionAggregateFunction(config), new SessionWindowProcessFunction())
                    .name("session-score-aggregate")
                    .map(score -> {
                        System.out.println("会话得分 - UID: " + score.uid + ", ItemID: " + score.rightItemId +
                                ", 总分: " + score.totalScore + ", 窗口结束: " + score.windowEndTs);
                        return score;
                    })
                    .name("debug-session-scores")
                    .filter((FilterFunction<SessionInteractionScore>) session -> {
                        boolean valid = session.totalScore > 0;
                        if (!valid) {
                            System.out.println("过滤零分会话 - UID: " + session.uid + ", ItemID: " + session.rightItemId);
                        }
                        return valid;
                    });

            System.out.println("11. 开始用户历史关联...");
            DataStream<PairScoreEvent> pairCandidateStream = sessionScoreStream
                    .flatMap(new UserHistoryJoinFunction(config, historyRedisConfig))
                    .name("join-user-history")
                    .map(pair -> {
                        System.out.println("候选Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId +
                                ", 得分: " + pair.score + ", 时间: " + pair.eventTimestamp);
                        return pair;
                    })
                    .name("debug-pair-candidates")
                    .filter((FilterFunction<PairScoreEvent>) pair -> {
                        boolean valid = pair.leftItemId > 0 && pair.rightItemId > 0;
                        if (!valid) {
                            System.out.println("过滤无效Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId);
                        }
                        return valid;
                    });

            System.out.println("12. 开始Pair窗口聚合...");
            DataStream<PairScoreEvent> pairWindowedStream = pairCandidateStream
                    .keyBy((KeySelector<PairScoreEvent, String>) pair -> {
                        String key = pair.pairKey();
                        System.out.println("KeyBy Pair: " + key);
                        return key;
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.minutes(config.getPairWindowMinutes())))
                    .reduce(new PairScoreReducer(), new PairWindowProcessFunction())
                    .name("pair-window-aggregate")
                    .map(pair -> {
                        System.out.println("窗口聚合Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId +
                                ", 聚合得分: " + pair.score + ", 时间: " + pair.eventTimestamp);
                        return pair;
                    })
                    .name("debug-windowed-pairs")
                    .filter((FilterFunction<PairScoreEvent>) pair -> {
                        boolean valid = pair.score >= config.getPairMinScore();
                        if (!valid) {
                            System.out.println("过滤低分Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId +
                                    ", 得分: " + pair.score + ", 阈值: " + config.getPairMinScore());
                        }
                        return valid;
                    });

            System.out.println("13. 开始时间衰减处理...");
            DataStream<PairScoreEvent> pairWithDecayStream = pairWindowedStream
                    .flatMap(new PastPairCountDecayRedisFunction(config, pairRedisConfig))
                    .name("pair-decay-merge")
                    .map(pair -> {
                        System.out.println("衰减后Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId +
                                ", 衰减得分: " + pair.score);
                        return pair;
                    })
                    .name("debug-decayed-pairs")
                    .filter((FilterFunction<PairScoreEvent>) pair -> {
                        boolean valid = pair.score >= config.getPairMinScore();
                        if (!valid) {
                            System.out.println("过滤衰减后低分Pair - Left: " + pair.leftItemId + ", Right: " + pair.rightItemId +
                                    ", 得分: " + pair.score);
                        }
                        return valid;
                    });

            // 写入 pair 计数（用于时间衰减）
            System.out.println("14. 设置Pair计数Redis Sink...");
            pairWithDecayStream.addSink(new PairCountRedisSink(config, pairRedisConfig))
                    .name("redis-pair-count-sink");

            // 按 leftItem 聚合所有 rightItem，然后批量写入 index
            System.out.println("15. 开始LeftItem聚合...");
            DataStream<Tuple2<Long, List<Tuple2<Long, Long>>>> leftItemAggregatedStream = pairWithDecayStream
                    .map((MapFunction<PairScoreEvent, Tuple2<Long, List<Tuple2<Long, Long>>>>) pair -> {
                        Tuple2<Long, List<Tuple2<Long, Long>>> result =
                                Tuple2.of(pair.leftItemId, Collections.singletonList(Tuple2.of(pair.rightItemId, pair.score)));
                        System.out.println("LeftItem映射 - Left: " + result.f0 + ", Right数量: 1");
                        return result;
                    })
                    .returns(Types.TUPLE(Types.LONG, Types.LIST(Types.TUPLE(Types.LONG, Types.LONG))))
                    .keyBy((KeySelector<Tuple2<Long, List<Tuple2<Long, Long>>>, Long>) value -> {
                        System.out.println("KeyBy LeftItem: " + value.f0);
                        return value.f0;
                    })
                    .window(TumblingProcessingTimeWindows.of(Time.minutes(config.getPairWindowMinutes())))
                    .reduce((ReduceFunction<Tuple2<Long, List<Tuple2<Long, Long>>>>) (a, b) -> {
                        int before = a.f1.size();
                        a.f1.addAll(b.f1);
                        System.out.println("LeftItem聚合 - Left: " + a.f0 + ", 合并前: " + before + ", 合并后: " + a.f1.size());
                        return a;
                    })
                    .name("left-item-aggregate")
                    .map(value -> {
                        System.out.println("聚合结果 - Left: " + value.f0 + ", Right数量: " + value.f1.size());
                        return value;
                    })
                    .name("debug-left-aggregated");

            System.out.println("16. 设置TopK Redis Sink...");
            leftItemAggregatedStream.addSink(new IndexTopKRedisSink(config, pairRedisConfig))
                    .name("redis-index-topk-sink");

            System.out.println("=== 开始执行Flink任务 ===");
            System.out.println("执行时间: " + new Date());
            env.execute("gosh-onlinecf");

        } catch (Exception e) {
            System.err.println("!!! OnlineCFJob 执行异常 !!!");
            System.err.println("异常时间: " + new Date());
            e.printStackTrace();
            LOG.error("OnlineCFJob 执行失败", e);
            throw e;
        }

        System.out.println("=== OnlineCFJob 正常结束 ===");
        System.out.println("结束时间: " + new Date());
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

        @Override
        public String toString() {
            return String.format("UserInteractionEvent{uid=%d, itemId=%d, sessionKey=%s, timestamp=%d}",
                    uid, itemId, sessionKey(), timestamp);
        }
    }

    private static class SessionAccumulator {
        long uid;
        long itemId;
        String recToken;
        long score;
        long latestTs;

        @Override
        public String toString() {
            return String.format("SessionAccumulator{uid=%d, itemId=%d, score=%d, latestTs=%d}",
                    uid, itemId, score, latestTs);
        }
    }

    private static class SessionInteractionScore {
        long uid;
        long rightItemId;
        long totalScore;
        long windowEndTs;
        String recToken;

        @Override
        public String toString() {
            return String.format("SessionInteractionScore{uid=%d, rightItemId=%d, totalScore=%d, windowEndTs=%d}",
                    uid, rightItemId, totalScore, windowEndTs);
        }
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

        @Override
        public String toString() {
            return String.format("PairScoreEvent{left=%d, right=%d, score=%d, timestamp=%d}",
                    leftItemId, rightItemId, score, eventTimestamp);
        }
    }

    private static class ItemHistory {
        long itemId;
        long timestamp;
        double weight;

        @Override
        public String toString() {
            return String.format("ItemHistory{itemId=%d, timestamp=%d, weight=%.2f}",
                    itemId, timestamp, weight);
        }
    }

    // ------------------------------------------------------------------------
    // Parser & Aggregators - 添加调试信息
    // ------------------------------------------------------------------------

    private static class FeatureEventToInteractionMapper implements MapFunction<UserFeatureEvent, UserInteractionEvent> {
        private transient int processedCount = 0;

        @Override
        public UserInteractionEvent map(UserFeatureEvent event) {
            processedCount++;
            UserInteractionEvent interaction = new UserInteractionEvent();
            if (event == null) {
                System.out.println("FeatureEventToInteractionMapper: 输入事件为null");
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

            System.out.println(String.format("FeatureEventToInteractionMapper[%d]: 转换完成 - %s",
                    processedCount, interaction));
            return interaction;
        }
    }

    private static class SessionAggregateFunction implements AggregateFunction<UserInteractionEvent, SessionAccumulator, SessionAccumulator> {

        private final OnlineCFConfig config;
        private transient int aggregateCount = 0;

        SessionAggregateFunction(OnlineCFConfig config) {
            this.config = config;
        }

        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(UserInteractionEvent value, SessionAccumulator acc) {
            aggregateCount++;
            acc.uid = value.uid;
            acc.itemId = value.itemId;
            acc.recToken = value.recToken;
            acc.latestTs = Math.max(acc.latestTs, value.timestamp);
            long score = computeScore(value);
            acc.score += score;

            System.out.println(String.format("SessionAggregateFunction[%d]: 添加事件 - UID: %d, Item: %d, 得分: %d, 累计得分: %d",
                    aggregateCount, value.uid, value.itemId, score, acc.score));
            return acc;
        }

        @Override
        public SessionAccumulator getResult(SessionAccumulator accumulator) {
            System.out.println(String.format("SessionAggregateFunction: 获取结果 - %s", accumulator));
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

            System.out.println(String.format("SessionAggregateFunction: 合并累加器 - A: %s, B: %s, 合并: %s",
                    a, b, merged));
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
        private transient int windowCount = 0;

        @Override
        public void process(String key,
                            Context context,
                            Iterable<SessionAccumulator> elements,
                            Collector<SessionInteractionScore> out) {
            windowCount++;
            SessionAccumulator acc = elements.iterator().next();
            if (acc == null || acc.uid <= 0 || acc.itemId <= 0 || acc.score <= 0) {
                System.out.println(String.format("SessionWindowProcessFunction[%d]: 无效累加器 - %s", windowCount, acc));
                return;
            }

            SessionInteractionScore score = new SessionInteractionScore();
            score.uid = acc.uid;
            score.rightItemId = acc.itemId;
            score.recToken = acc.recToken;
            score.totalScore = acc.score;
            score.windowEndTs = context.window().getEnd();

            System.out.println(String.format("SessionWindowProcessFunction[%d]: 窗口处理完成 - %s", windowCount, score));
            out.collect(score);
        }
    }

    private static class PairScoreReducer implements ReduceFunction<PairScoreEvent> {
        @Override
        public PairScoreEvent reduce(PairScoreEvent value1, PairScoreEvent value2) {
            System.out.println(String.format("PairScoreReducer: 合并Pair - A: %s, B: %s", value1, value2));
            value1.score += value2.score;
            value1.eventTimestamp = Math.max(value1.eventTimestamp, value2.eventTimestamp);
            System.out.println(String.format("PairScoreReducer: 合并结果 - %s", value1));
            return value1;
        }
    }

    private static class PairWindowProcessFunction extends ProcessWindowFunction<PairScoreEvent, PairScoreEvent, String, TimeWindow> {
        private transient int pairWindowCount = 0;

        @Override
        public void process(String key,
                            Context context,
                            Iterable<PairScoreEvent> elements,
                            Collector<PairScoreEvent> out) {
            pairWindowCount++;
            PairScoreEvent event = elements.iterator().next();
            if (event != null) {
                event.eventTimestamp = context.window().getEnd();
                System.out.println(String.format("PairWindowProcessFunction[%d]: 窗口处理完成 - %s", pairWindowCount, event));
                out.collect(event);
            } else {
                System.out.println(String.format("PairWindowProcessFunction[%d]: 窗口内无有效事件", pairWindowCount));
            }
        }
    }

    // ------------------------------------------------------------------------
    // Redis interactions - 添加详细调试
    // ------------------------------------------------------------------------

    private static class UserHistoryJoinFunction extends RichFlatMapFunction<SessionInteractionScore, PairScoreEvent> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private final UserHistoryParser parser;
        private transient RedisHelper redisHelper;
        private transient int joinCount = 0;

        public UserHistoryJoinFunction(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
            this.parser = new UserHistoryParser(config);
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            System.out.println("UserHistoryJoinFunction: 初始化开始...");
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
            System.out.println("UserHistoryJoinFunction: Redis连接初始化完成");
        }

        @Override
        public void flatMap(SessionInteractionScore value,
                            Collector<PairScoreEvent> out) throws Exception {
            joinCount++;
            System.out.println(String.format("UserHistoryJoinFunction[%d]: 开始处理 - %s", joinCount, value));

            if (value == null || value.uid <= 0) {
                System.out.println(String.format("UserHistoryJoinFunction[%d]: 无效输入", joinCount));
                return;
            }

            String redisKey = config.getHistoryKeyPrefix() + value.uid + config.getHistoryKeySuffix();
            System.out.println(String.format("UserHistoryJoinFunction[%d]: 读取Redis Key - %s", joinCount, redisKey));

            byte[] raw = redisHelper.getValue(redisKey);
            if (raw == null || raw.length == 0) {
                System.out.println(String.format("UserHistoryJoinFunction[%d]: Redis中无用户历史数据", joinCount));
                return;
            }

            System.out.println(String.format("UserHistoryJoinFunction[%d]: 获取到历史数据，大小: %d bytes", joinCount, raw.length));
            List<ItemHistory> histories = parser.parse(raw);
            System.out.println(String.format("UserHistoryJoinFunction[%d]: 解析出历史记录数量: %d", joinCount, histories.size()));

            if (histories.isEmpty()) {
                System.out.println(String.format("UserHistoryJoinFunction[%d]: 历史记录为空", joinCount));
                return;
            }

            int pairCount = 0;
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

                System.out.println(String.format("UserHistoryJoinFunction[%d]: 生成Pair - %s, 历史权重: %.2f",
                        joinCount, pair, history.weight));
                out.collect(pair);
                pairCount++;
            }

            System.out.println(String.format("UserHistoryJoinFunction[%d]: 处理完成，生成 %d 个Pair", joinCount, pairCount));
        }

        @Override
        public void close() {
            System.out.println("UserHistoryJoinFunction: 关闭，总共处理: " + joinCount + " 次连接");
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    private static class PastPairCountDecayRedisFunction extends RichFlatMapFunction<PairScoreEvent, PairScoreEvent> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private transient RedisHelper redisHelper;
        private transient int decayCount = 0;

        PastPairCountDecayRedisFunction(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            System.out.println("PastPairCountDecayRedisFunction: 初始化开始...");
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
            System.out.println("PastPairCountDecayRedisFunction: Redis连接初始化完成");
        }

        @Override
        public void flatMap(PairScoreEvent value, Collector<PairScoreEvent> out) {
            decayCount++;
            System.out.println(String.format("PastPairCountDecayRedisFunction[%d]: 开始处理 - %s", decayCount, value));

            if (value == null) {
                System.out.println(String.format("PastPairCountDecayRedisFunction[%d]: 无效输入", decayCount));
                return;
            }

            String key = config.getPairKeyPrefix() + value.leftItemId + "_" + value.rightItemId;
            System.out.println(String.format("PastPairCountDecayRedisFunction[%d]: 查询Redis Key - %s", decayCount, key));

            String cached = redisHelper.getStringValue(key);
            long currentMinute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            long decayed = countDecay(cached, config.getDecayWeight(), config.getDecayStepMinutes(), currentMinute);

            System.out.println(String.format("PastPairCountDecayRedisFunction[%d]: 衰减计算 - 缓存值: %s, 当前分钟: %d, 衰减得分: %d",
                    decayCount, cached, currentMinute, decayed));

            long originalScore = value.score;
            value.score += decayed;

            System.out.println(String.format("PastPairCountDecayRedisFunction[%d]: 得分更新 - 原始: %d, 衰减后: %d, 最终: %d",
                    decayCount, originalScore, decayed, value.score));

            out.collect(value);
        }

        @Override
        public void close() {
            System.out.println("PastPairCountDecayRedisFunction: 关闭，总共处理: " + decayCount + " 次衰减");
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
        private transient int sinkCount = 0;

        PairCountRedisSink(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            System.out.println("PairCountRedisSink: 初始化开始...");
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
            System.out.println("PairCountRedisSink: Redis连接初始化完成");
        }

        @Override
        public void invoke(PairScoreEvent value, Context context) {
            sinkCount++;
            System.out.println(String.format("PairCountRedisSink[%d]: 开始写入 - %s", sinkCount, value));

            if (value == null || value.leftItemId <= 0 || value.rightItemId <= 0) {
                System.out.println(String.format("PairCountRedisSink[%d]: 无效数据，跳过写入", sinkCount));
                return;
            }

            long currentMinute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            String pairKey = config.getPairKeyPrefix() + value.leftItemId + "_" + value.rightItemId;
            String pairValue = value.score + ":" + currentMinute;

            System.out.println(String.format("PairCountRedisSink[%d]: 写入Redis - Key: %s, Value: %s, TTL: %d",
                    sinkCount, pairKey, pairValue, config.getPairExpireSeconds()));

            redisHelper.setex(pairKey, config.getPairExpireSeconds(), pairValue);
            System.out.println(String.format("PairCountRedisSink[%d]: 写入完成", sinkCount));
        }

        @Override
        public void close() {
            System.out.println("PairCountRedisSink: 关闭，总共写入: " + sinkCount + " 条记录");
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    /**
     * 批量写入 index Top-K 到 Redis ZSet
     */
    private static class IndexTopKRedisSink extends RichSinkFunction<Tuple2<Long, List<Tuple2<Long, Long>>>> {

        private final OnlineCFConfig config;
        private final RedisConfig redisConfig;
        private transient RedisHelper redisHelper;
        private transient int indexSinkCount = 0;

        IndexTopKRedisSink(OnlineCFConfig config, RedisConfig redisConfig) {
            this.config = config;
            this.redisConfig = redisConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            System.out.println("IndexTopKRedisSink: 初始化开始...");
            this.redisHelper = new RedisHelper(redisConfig);
            this.redisHelper.open();
            System.out.println("IndexTopKRedisSink: Redis连接初始化完成");
        }

        @Override
        public void invoke(Tuple2<Long, List<Tuple2<Long, Long>>> value, Context context) {
            indexSinkCount++;
            System.out.println(String.format("IndexTopKRedisSink[%d]: 开始处理 - Left: %d, Right数量: %d",
                    indexSinkCount, value.f0, value.f1.size()));

            if (value == null || value.f0 == null || value.f1 == null || value.f1.isEmpty()) {
                System.out.println(String.format("IndexTopKRedisSink[%d]: 无效数据，跳过处理", indexSinkCount));
                return;
            }

            long leftItemId = value.f0;
            String indexKey = config.getIndexKeyPrefix() + leftItemId;
            System.out.println(String.format("IndexTopKRedisSink[%d]: 处理Index Key - %s", indexSinkCount, indexKey));

            // 1. 读取现有的 Top-K
            System.out.println(String.format("IndexTopKRedisSink[%d]: 读取现有Top-K", indexSinkCount));
            List<String> existingTopK = redisHelper.getTopK(indexKey, config.getIndexLimit());
            System.out.println(String.format("IndexTopKRedisSink[%d]: 现有Top-K数量: %d", indexSinkCount, existingTopK.size()));

            List<Long> newRightItemIds = value.f1.stream()
                    .map(pair -> pair.f0)
                    .collect(Collectors.toList());
            System.out.println(String.format("IndexTopKRedisSink[%d]: 新RightItem数量: %d", indexSinkCount, newRightItemIds.size()));

            List<Tuple2<Long, Long>> existingPairs = new ArrayList<>();
            for (String member : existingTopK) {
                try {
                    long rightItemId = Long.parseLong(member);
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
            System.out.println(String.format("IndexTopKRedisSink[%d]: 保留的历史Pair数量: %d", indexSinkCount, existingPairs.size()));

            // 2. 合并新数据和历史数据
            List<Tuple2<Long, Long>> allPairs = new ArrayList<>(value.f1);
            allPairs.addAll(existingPairs);
            System.out.println(String.format("IndexTopKRedisSink[%d]: 合并后总Pair数量: %d", indexSinkCount, allPairs.size()));

            // 3. 按 score 从高到低排序，去重并保留最高分
            Map<Long, Long> scoreMap = new HashMap<>();
            for (Tuple2<Long, Long> pair : allPairs) {
                long rightItemId = pair.f0;
                long score = pair.f1;
                scoreMap.merge(rightItemId, score, Math::max);
            }
            System.out.println(String.format("IndexTopKRedisSink[%d]: 去重后唯一Pair数量: %d", indexSinkCount, scoreMap.size()));

            // 4. 按 score 从高到低排序，过滤并限制 Top-K
            List<Map.Entry<Long, Long>> sorted = scoreMap.entrySet().stream()
                    .filter(entry -> entry.getValue() >= config.getIndexMinScore())
                    .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                    .limit(config.getIndexLimit())
                    .collect(Collectors.toList());
            System.out.println(String.format("IndexTopKRedisSink[%d]: 过滤后Top-K数量: %d", indexSinkCount, sorted.size()));

            // 5. 删除旧的 ZSet，重新写入 Top-K
            System.out.println(String.format("IndexTopKRedisSink[%d]: 删除旧Index", indexSinkCount));
            redisHelper.del(indexKey);

            // 6. 批量写入 ZSet
            System.out.println(String.format("IndexTopKRedisSink[%d]: 开始写入新Index", indexSinkCount));
            for (Map.Entry<Long, Long> entry : sorted) {
                redisHelper.zadd(indexKey, entry.getValue().doubleValue(), String.valueOf(entry.getKey()));
            }

            // 7. 设置过期时间
            if (!sorted.isEmpty()) {
                redisHelper.expire(indexKey, config.getIndexExpireSeconds());
                System.out.println(String.format("IndexTopKRedisSink[%d]: 设置过期时间: %d秒", indexSinkCount, config.getIndexExpireSeconds()));
            } else {
                System.out.println(String.format("IndexTopKRedisSink[%d]: 无有效数据，不设置过期时间", indexSinkCount));
            }

            System.out.println(String.format("IndexTopKRedisSink[%d]: 处理完成", indexSinkCount));
        }

        @Override
        public void close() {
            System.out.println("IndexTopKRedisSink: 关闭，总共处理: " + indexSinkCount + " 个Index");
            Optional.ofNullable(redisHelper).ifPresent(RedisHelper::close);
        }
    }

    // ------------------------------------------------------------------------
    // Helper classes - 添加调试信息
    // ------------------------------------------------------------------------

    private static class UserHistoryParser {

        private final OnlineCFConfig config;

        UserHistoryParser(OnlineCFConfig config) {
            this.config = config;
        }

        List<ItemHistory> parse(byte[] payload) {
            System.out.println("UserHistoryParser: 开始解析历史数据，大小: " + payload.length + " bytes");

            // First try to parse protobuf RecUserFeature
            try {
                RecFeature.RecUserFeature feature = RecFeature.RecUserFeature.parseFrom(payload);
                System.out.println("UserHistoryParser: Protobuf解析成功");

                List<ItemHistory> result = new ArrayList<>();
                mergeHistory(result, feature.getViewer3SviewPostHis24H(), 1.0);
                mergeHistory(result, feature.getViewer5SstandPostHis24H(), 1.0);
                mergeHistory(result, feature.getViewerLikePostHis24H(), 1.2);
                mergeHistory(result, feature.getViewerFollowPostHis24H(), 1.3);
                mergeHistory(result, feature.getViewerProfilePostHis24H(), 0.8);
                mergeHistory(result, feature.getViewerPosinterPostHis24H(), 1.1);

                System.out.println("UserHistoryParser: Protobuf解析完成，原始数量: " + result.size());
                return limitAndFilter(result);
            } catch (Exception protoError) {
                System.out.println("UserHistoryParser: Protobuf解析失败，尝试文本解析: " + protoError.getMessage());
                String raw = new String(payload, StandardCharsets.UTF_8);
                return parsePlainString(raw);
            }
        }

        private void mergeHistory(List<ItemHistory> target, String raw, double baseWeight) {
            if (StringUtils.isBlank(raw)) {
                return;
            }
            String[] tokens = raw.split(",");
            System.out.println("UserHistoryParser: 合并历史 - 原始字符串: " + raw + ", 分割数量: " + tokens.length);

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

                System.out.println("UserHistoryParser: 添加历史记录 - " + history);
            }
        }

        private List<ItemHistory> parsePlainString(String raw) {
            System.out.println("UserHistoryParser: 开始文本解析 - " + (raw.length() > 200 ? raw.substring(0, 200) + "..." : raw));
            List<ItemHistory> result = new ArrayList<>();
            if (StringUtils.isBlank(raw)) {
                return result;
            }
            try {
                JsonNode arrayNode = OBJECT_MAPPER.readTree(raw);
                if (arrayNode.isArray()) {
                    System.out.println("UserHistoryParser: JSON数组解析，元素数量: " + arrayNode.size());
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
                    System.out.println("UserHistoryParser: JSON解析完成，数量: " + result.size());
                    return limitAndFilter(result);
                }
            } catch (IOException ignore) {
                System.out.println("UserHistoryParser: JSON解析失败，回退到简单解析");
            }
            mergeHistory(result, raw, 1.0);
            return limitAndFilter(result);
        }

        private List<ItemHistory> limitAndFilter(List<ItemHistory> input) {
            long expireMillis = TimeUnit.HOURS.toMillis(config.getHistoryMaxAgeHours());
            long now = System.currentTimeMillis();
            System.out.println("UserHistoryParser: 开始过滤限制 - 输入数量: " + input.size() + ", 最大年龄: " + config.getHistoryMaxAgeHours() + "小时");

            List<ItemHistory> filtered = input.stream()
                    .filter(history -> history.itemId > 0)
                    .filter(history -> expireMillis <= 0 || now - history.timestamp <= expireMillis)
                    .sorted(Comparator.comparingLong((ItemHistory h) -> h.timestamp).reversed())
                    .collect(Collectors.toList());

            System.out.println("UserHistoryParser: 时间过滤后数量: " + filtered.size());

            LinkedHashMap<Long, ItemHistory> dedup = new LinkedHashMap<>();
            for (ItemHistory history : filtered) {
                dedup.putIfAbsent(history.itemId, history);
            }

            List<ItemHistory> result = dedup.values().stream().limit(config.getHistoryMaxItems()).collect(Collectors.toList());
            System.out.println("UserHistoryParser: 去重限制后最终数量: " + result.size());
            return result;
        }
    }

    private static long countDecay(String storedValue, double decayWeight, int decayStepMinutes, long currentMinuteTs) {
        if (StringUtils.isBlank(storedValue)) {
            System.out.println("countDecay: 存储值为空，返回0");
            return 0L;
        }
        String[] parts = storedValue.split(":");
        if (parts.length < 2) {
            System.out.println("countDecay: 存储值格式错误: " + storedValue);
            return 0L;
        }
        long cachedScore = NumberUtils.toLong(parts[0], 0L);
        long cachedMinute = NumberUtils.toLong(parts[1], 0L);
        long steps = Math.max(0L, currentMinuteTs - cachedMinute);
        double stepSize = decayStepMinutes <= 0 ? 1.0 : decayStepMinutes;
        double factor = Math.pow(decayWeight, steps / stepSize);
        long result = Math.round(cachedScore * factor);

        System.out.println(String.format("countDecay: 缓存值: %s, 当前分钟: %d, 缓存分钟: %d, 步数: %d, 衰减因子: %.4f, 结果: %d",
                storedValue, currentMinuteTs, cachedMinute, steps, factor, result));
        return result;
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
            System.out.println("RedisHelper: 开始初始化Redis连接");
            this.connectionManager = RedisConnectionManager.getInstance(redisConfig);
            this.clusterMode = redisConfig.isClusterMode();
            if (clusterMode) {
                this.clusterCommands = connectionManager.getRedisClusterCommands();
                System.out.println("RedisHelper: 集群模式初始化完成");
            } else {
                this.commands = connectionManager.getRedisCommands();
                System.out.println("RedisHelper: 单机模式初始化完成");
            }
        }

        byte[] getValue(String key) {
            System.out.println("RedisHelper: GET值 - Key: " + key);
            try {
                Tuple2<String, byte[]> result = clusterMode
                        ? clusterCommands.get(key)
                        : commands.get(key);
                if (result == null) {
                    System.out.println("RedisHelper: GET返回null");
                    return null;
                }
                System.out.println("RedisHelper: GET成功，数据大小: " + (result.f1 != null ? result.f1.length : 0) + " bytes");
                return result.f1;
            } catch (Exception e) {
                System.err.println("RedisHelper: GET操作异常 - " + e.getMessage());
                return null;
            }
        }

        String getStringValue(String key) {
            byte[] value = getValue(key);
            String result = value == null ? null : new String(value, StandardCharsets.UTF_8);
            System.out.println("RedisHelper: 字符串值 - " + result);
            return result;
        }

        void setex(String key, int ttlSeconds, String value) {
            System.out.println(String.format("RedisHelper: SETEX - Key: %s, TTL: %d, Value: %s", key, ttlSeconds, value));
            try {
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
                System.out.println("RedisHelper: SETEX操作完成");
            } catch (Exception e) {
                System.err.println("RedisHelper: SETEX操作异常 - " + e.getMessage());
            }
        }

        void zadd(String key, double score, String member) {
            System.out.println(String.format("RedisHelper: ZADD - Key: %s, Score: %.2f, Member: %s", key, score, member));
            try {
                Tuple2<String, byte[]> tuple = wrap(member);
                if (clusterMode) {
                    clusterCommands.zadd(key, score, tuple);
                } else {
                    commands.zadd(key, score, tuple);
                }
            } catch (Exception e) {
                System.err.println("RedisHelper: ZADD操作异常 - " + e.getMessage());
            }
        }

        long zcard(String key) {
            System.out.println("RedisHelper: ZCARD - Key: " + key);
            try {
                long result = clusterMode ? clusterCommands.zcard(key) : commands.zcard(key);
                System.out.println("RedisHelper: ZCARD结果: " + result);
                return result;
            } catch (Exception e) {
                System.err.println("RedisHelper: ZCARD操作异常 - " + e.getMessage());
                return 0;
            }
        }

        void zremRangeByRank(String key, long start, long end) {
            System.out.println(String.format("RedisHelper: ZREMRANGEBYRANK - Key: %s, Start: %d, End: %d", key, start, end));
            try {
                if (clusterMode) {
                    clusterCommands.zremrangebyrank(key, start, end);
                } else {
                    commands.zremrangebyrank(key, start, end);
                }
            } catch (Exception e) {
                System.err.println("RedisHelper: ZREMRANGEBYRANK操作异常 - " + e.getMessage());
            }
        }

        List<String> zrevrange(String key, long start, long end) {
            System.out.println(String.format("RedisHelper: ZREVRANGE - Key: %s, Start: %d, End: %d", key, start, end));
            try {
                List<Tuple2<String, byte[]>> result;
                if (clusterMode) {
                    result = clusterCommands.zrevrange(key, start, end);
                } else {
                    result = commands.zrevrange(key, start, end);
                }
                List<String> members = result.stream()
                        .map(tuple -> new String(tuple.f1, StandardCharsets.UTF_8))
                        .collect(Collectors.toList());
                System.out.println("RedisHelper: ZREVRANGE结果数量: " + members.size());
                return members;
            } catch (Exception e) {
                System.err.println("RedisHelper: ZREVRANGE操作异常 - " + e.getMessage());
                return Collections.emptyList();
            }
        }

        Double zscore(String key, String member) {
            System.out.println(String.format("RedisHelper: ZSCORE - Key: %s, Member: %s", key, member));
            try {
                Tuple2<String, byte[]> tuple = wrap(member);
                Double result = clusterMode ? clusterCommands.zscore(key, tuple) : commands.zscore(key, tuple);
                System.out.println("RedisHelper: ZSCORE结果: " + result);
                return result;
            } catch (Exception e) {
                System.err.println("RedisHelper: ZSCORE操作异常 - " + e.getMessage());
                return null;
            }
        }

        List<String> getTopK(String key, int limit) {
            System.out.println(String.format("RedisHelper: 获取TopK - Key: %s, Limit: %d", key, limit));
            return zrevrange(key, 0, limit - 1);
        }

        void expire(String key, int ttlSeconds) {
            if (ttlSeconds <= 0) {
                return;
            }
            System.out.println(String.format("RedisHelper: EXPIRE - Key: %s, TTL: %d", key, ttlSeconds));
            try {
                if (clusterMode) {
                    clusterCommands.expire(key, ttlSeconds);
                } else {
                    commands.expire(key, ttlSeconds);
                }
            } catch (Exception e) {
                System.err.println("RedisHelper: EXPIRE操作异常 - " + e.getMessage());
            }
        }

        void del(String key) {
            System.out.println("RedisHelper: DEL - Key: " + key);
            try {
                if (clusterMode) {
                    clusterCommands.del(key);
                } else {
                    commands.del(key);
                }
            } catch (Exception e) {
                System.err.println("RedisHelper: DEL操作异常 - " + e.getMessage());
            }
        }

        void close() {
            System.out.println("RedisHelper: 关闭Redis连接");
            if (connectionManager != null) {
                connectionManager.shutdown();
            }
        }

        private Tuple2<String, byte[]> wrap(String value) {
            return new Tuple2<>("", value.getBytes(StandardCharsets.UTF_8));
        }
    }

    // ------------------------------------------------------------------------
    // Configuration - 保持不变
    // ------------------------------------------------------------------------

    static class OnlineCFConfig {
        // ... 配置内容保持不变 ...
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