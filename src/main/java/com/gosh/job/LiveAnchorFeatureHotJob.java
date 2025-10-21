package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.EventFilterUtil;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 直播主播特征生产任务（包含热度等特征）
 * 数据源：Kafka topic=advertise, event_type=1
 * 处理5种事件：进房、退房、聊天、送礼、关注
 * 生成特征：按anchor_id聚合，统计7组热度指标
 * 时间窗口：5min/10min/15min，滑动间隔10秒
 * 关联逻辑：其他事件通过uid与进房事件关联（不需要live_id）
 */
public class LiveAnchorFeatureHotJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveAnchorFeatureHotJob.class);
    
    // Redis key前缀和后缀
    private static final String PREFIX = "rec:anchor_feature:{";
    private static final String SUFFIX_5MIN = "}:live5min";
    private static final String SUFFIX_10MIN = "}:live10min";
    private static final String SUFFIX_15MIN = "}:live15min";
    
    // 事件类型常量
    private static final String EVENT_ENTER_LIVEROOM = "enter_liveroom";
    private static final String EVENT_EXIT_LIVEROOM = "exit_liveroom";
    private static final String EVENT_LIVEROOM_CHAT = "liveroom_chat_message";
    private static final String EVENT_LIVEROOM_GIFT = "liveroom_gift_send";
    private static final String EVENT_LIVEROOM_FOLLOW = "liveroom_follow_anchor";

    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Starting LiveAnchorFeatureHotJob");
        LOG.info("Processing event_type=1 for live room hot features");
        LOG.info("Target events: enter_liveroom, exit_liveroom, liveroom_chat_message, liveroom_gift_send, liveroom_follow_anchor");
        LOG.info("Windows: 5min/10min/15min, slide interval: 10s");
        LOG.info("Redis key prefix: {}", PREFIX);
        LOG.info("========================================");
        
        // 第一步：创建Flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        LOG.info("Flink environment created");

        // 第二步：创建Kafka Source（topic=advertise）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "advertise"
        );
        LOG.info("Kafka source created for topic: advertise");

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        LOG.info("Kafka data stream created");

        // 第四步：预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)");

        // 第五步：解析事件（进房、退房、聊天、送礼、关注）
        SingleOutputStreamOperator<LiveRoomEvent> eventStream = filteredStream
            .flatMap(new LiveRoomEventParser())
            .name("Parse Live Room Events");

        // 第六步：为3个时间窗口创建数据流并写入Redis
        
        // 5分钟窗口
        processWindowAndSinkToRedis(env, eventStream, 5, SUFFIX_5MIN);
        
        // 10分钟窗口
        processWindowAndSinkToRedis(env, eventStream, 10, SUFFIX_10MIN);
        
        // 15分钟窗口
        processWindowAndSinkToRedis(env, eventStream, 15, SUFFIX_15MIN);

        LOG.info("Job configured with 3 time windows (5/10/15 min), starting execution...");
        env.execute("Live Anchor Hot Feature Job");
    }

    /**
     * 处理指定时间窗口并写入Redis
     */
    private static void processWindowAndSinkToRedis(
            StreamExecutionEnvironment env,
            DataStream<LiveRoomEvent> eventStream,
            int windowMinutes,
            String redisSuffix) {
        
        // 按 anchor_id 分组并进行滑动窗口聚合（窗口大小=windowMinutes，滑动间隔=10秒）
        DataStream<AnchorHotFeatureAgg> aggregatedStream = eventStream
            .keyBy(new KeySelector<LiveRoomEvent, Long>() {
                @Override
                public Long getKey(LiveRoomEvent value) throws Exception {
                    return value.anchorId;
                }
            })
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(windowMinutes),
                Time.seconds(10)
            ))
            .aggregate(new AnchorHotFeatureAggregator(windowMinutes))
            .name("Anchor Hot Feature Aggregation " + windowMinutes + "min")
            .process(new org.apache.flink.streaming.api.functions.ProcessFunction<AnchorHotFeatureAgg, AnchorHotFeatureAgg>() {
                private transient long aggCount = 0;
                private transient long lastLogTime = 0;
                
                @Override
                public void processElement(AnchorHotFeatureAgg value, Context ctx, Collector<AnchorHotFeatureAgg> out) throws Exception {
                    aggCount++;
                    long now = System.currentTimeMillis();
                    // 每100个窗口或每5分钟输出一次
                    if (aggCount % 100 == 0 || now - lastLogTime > 300000) {
                        lastLogTime = now;
                        LOG.info("[Window Agg {}min] Total windows: {}, sample: anchorId={}, enter={}, quit={}, avgDuration={}s, giftCoin={}, giftUser={}, follow={}, chat={}", 
                            windowMinutes, aggCount, value.anchorId, value.enterUserCount, value.quitUserCount, 
                            value.avgQuitDuration, value.giftCoin, value.giftUserCount, value.followUserCount, value.chatUserCount);
                    }
                    out.collect(value);
                }
            })
            .name("Agg Monitor " + windowMinutes + "min");

        // 转换为Protobuf并写入Redis
        DataStream<Tuple2<String, byte[]>> dataStream = aggregatedStream
            .filter(agg -> agg != null && agg.anchorId > 0)
            .map(new MapFunction<AnchorHotFeatureAgg, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(AnchorHotFeatureAgg agg) throws Exception {
                    String redisKey = PREFIX + agg.anchorId + redisSuffix;
                    byte[] protobufBytes = buildProtobuf(agg, windowMinutes);
                    return new Tuple2<>(redisKey, protobufBytes);
                }
            })
            .name("Convert to Protobuf " + windowMinutes + "min");

        // 添加Redis写入监控日志
        dataStream
            .process(new org.apache.flink.streaming.api.functions.ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>() {
                private transient long writeCount = 0;
                private transient long lastLogTime = 0;
                
                @Override
                public void processElement(Tuple2<String, byte[]> value, Context ctx, Collector<Tuple2<String, byte[]>> out) throws Exception {
                    writeCount++;
                    long now = System.currentTimeMillis();
                    // 每5分钟输出一次统计
                    if (now - lastLogTime > 300000) {
                        lastLogTime = now;
                        LOG.info("[Redis Write {}min] Total writes: {}, latest key: {}, bytesSize: {}", 
                            windowMinutes, writeCount, value.f0, value.f1 != null ? value.f1.length : 0);
                    }
                    out.collect(value);
                }
            })
            .name("Redis Write Monitor " + windowMinutes + "min");

        // 创建Redis Sink（TTL=1小时）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(3600 * 1);
        redisConfig.setCommand("SET");
        LOG.info("Redis config for {}min window: TTL={}, Command={}", 
            windowMinutes, redisConfig.getTtl(), redisConfig.getCommand());
        
        RedisUtil.addRedisSink(
            dataStream,
            redisConfig,
            true,
            100
        );
    }

    /**
     * 构建Protobuf字节数组
     */
    private static byte[] buildProtobuf(AnchorHotFeatureAgg agg, int windowMinutes) {
        RecFeature.RecLiveAnchorFeature.Builder builder = RecFeature.RecLiveAnchorFeature.newBuilder()
            .setAnchorId(agg.anchorId);
        
        // 根据窗口类型设置对应的字段
        if (windowMinutes == 5) {
            builder.setLiveEnterUsernum5Min(agg.enterUserCount)
                   .setLiveQuitUsernum5Min(agg.quitUserCount)
                   .setLiveQuitAvgDuration5Min(agg.avgQuitDuration)
                   .setLiveAnchorGiftCoin5Min(agg.giftCoin)
                   .setLiveAnchorGiftUsernum5Min(agg.giftUserCount)
                   .setLiveAnchorFollowUsernum5Min(agg.followUserCount)
                   .setLiveAnchorChatUsernum5Min(agg.chatUserCount);
        } else if (windowMinutes == 10) {
            builder.setLiveEnterUsernum10Min(agg.enterUserCount)
                   .setLiveQuitUsernum10Min(agg.quitUserCount)
                   .setLiveQuitAvgDuration10Min(agg.avgQuitDuration)
                   .setLiveAnchorGiftCoin10Min(agg.giftCoin)
                   .setLiveAnchorGiftUsernum10Min(agg.giftUserCount)
                   .setLiveAnchorFollowUsernum10Min(agg.followUserCount)
                   .setLiveAnchorChatUsernum10Min(agg.chatUserCount);
        } else if (windowMinutes == 15) {
            builder.setLiveEnterUsernum15Min(agg.enterUserCount)
                   .setLiveQuitUsernum15Min(agg.quitUserCount)
                   .setLiveQuitAvgDuration15Min(agg.avgQuitDuration)
                   .setLiveAnchorGiftCoin15Min(agg.giftCoin)
                   .setLiveAnchorGiftUsernum15Min(agg.giftUserCount)
                   .setLiveAnchorFollowUsernum15Min(agg.followUserCount)
                   .setLiveAnchorChatUsernum15Min(agg.chatUserCount);
        }
        
        return builder.build().toByteArray();
    }

    /**
     * 直播间事件基类
     */
    public static class LiveRoomEvent {
        public long uid;
        public long liveId;
        public long anchorId;
        public String eventType;
        public long stayDuration;      // 退房事件的停留时长
        public int giftPrice;          // 送礼事件的礼物价格
        public int giftCount;          // 送礼事件的礼物数量
        
        public LiveRoomEvent(long uid, long liveId, long anchorId, String eventType) {
            this.uid = uid;
            this.liveId = liveId;
            this.anchorId = anchorId;
            this.eventType = eventType;
        }
    }

    /**
     * 直播间事件解析器
     * 解析5种事件类型：进房、退房、聊天、送礼、关注
     */
    public static class LiveRoomEventParser implements FlatMapFunction<String, LiveRoomEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static volatile long totalParsedEvents = 0;

        @Override
        public void flatMap(String value, Collector<LiveRoomEvent> out) throws Exception {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(value);
                
                // 获取 user_event_log 对象
                JsonNode userEventLog = root.path("user_event_log");
                if (userEventLog.isMissingNode()) {
                    return;
                }
                
                // 获取事件类型（在 user_event_log.event 中）
                JsonNode eventNode = userEventLog.path("event");
                if (eventNode.isMissingNode() || !eventNode.isTextual()) {
                    return;
                }
                String event = eventNode.asText();
                
                // 只处理我们关心的5种事件
                if (!EVENT_ENTER_LIVEROOM.equals(event) && 
                    !EVENT_EXIT_LIVEROOM.equals(event) && 
                    !EVENT_LIVEROOM_CHAT.equals(event) && 
                    !EVENT_LIVEROOM_GIFT.equals(event) && 
                    !EVENT_LIVEROOM_FOLLOW.equals(event)) {
                    return;
                }
                
                // 获取 event_data 字段（在 user_event_log.event_data 中）
                JsonNode eventDataNode = userEventLog.path("event_data");
                if (eventDataNode.isMissingNode() || !eventDataNode.isTextual()) {
                    return;
                }
                
                String eventData = eventDataNode.asText();
                if (eventData == null || eventData.isEmpty() || "null".equals(eventData)) {
                    return;
                }
                
                // 解析 event_data 的JSON内容
                JsonNode data = OBJECT_MAPPER.readTree(eventData);
                
                long uid = data.path("uid").asLong(0);
                long liveId = data.path("live_id").asLong(0);
                long anchorId = data.path("anchor_id").asLong(0);
                
                if (uid <= 0 || anchorId <= 0) {
                    return;
                }
                
                LiveRoomEvent evt = new LiveRoomEvent(uid, liveId, anchorId, event);
                
                // 根据事件类型提取特定字段
                if (EVENT_EXIT_LIVEROOM.equals(event)) {
                    evt.stayDuration = data.path("stay_duration").asLong(0);
                } else if (EVENT_LIVEROOM_GIFT.equals(event)) {
                    evt.giftPrice = data.path("gift_price").asInt(0);
                    evt.giftCount = data.path("gift_count").asInt(0);
                }
                
                totalParsedEvents++;
                if (totalParsedEvents % 10000 == 0) {
                    LOG.info("[Parser] Total parsed events: {}", totalParsedEvents);
                }
                
                out.collect(evt);
            } catch (Exception e) {
                // 静默处理异常
            }
        }
    }

    /**
     * 主播热度特征聚合器
     */
    public static class AnchorHotFeatureAggregator implements AggregateFunction<LiveRoomEvent, AnchorHotAccumulator, AnchorHotFeatureAgg> {
        private final int windowMinutes;
        
        public AnchorHotFeatureAggregator(int windowMinutes) {
            this.windowMinutes = windowMinutes;
        }
        
        @Override
        public AnchorHotAccumulator createAccumulator() {
            return new AnchorHotAccumulator();
        }

        @Override
        public AnchorHotAccumulator add(LiveRoomEvent event, AnchorHotAccumulator acc) {
            acc.anchorId = event.anchorId;
            
            // 进房事件：记录uid以便与其他事件关联
            if (EVENT_ENTER_LIVEROOM.equals(event.eventType)) {
                acc.enterUids.add(event.uid);
            }
            // 退房事件：需要与进房事件关联（只用uid）
            else if (EVENT_EXIT_LIVEROOM.equals(event.eventType)) {
                if (acc.enterUids.contains(event.uid)) {
                    acc.quitUids.add(event.uid);
                    acc.quitDurations.add(event.stayDuration);
                }
            }
            // 送礼事件：需要与进房事件关联（只用uid）
            else if (EVENT_LIVEROOM_GIFT.equals(event.eventType)) {
                if (acc.enterUids.contains(event.uid)) {
                    acc.giftUids.add(event.uid);
                    acc.totalGiftCoin += (event.giftPrice * event.giftCount);
                }
            }
            // 关注事件：需要与进房事件关联（只用uid）
            else if (EVENT_LIVEROOM_FOLLOW.equals(event.eventType)) {
                if (acc.enterUids.contains(event.uid)) {
                    acc.followUids.add(event.uid);
                }
            }
            // 聊天事件：需要与进房事件关联（只用uid）
            else if (EVENT_LIVEROOM_CHAT.equals(event.eventType)) {
                if (acc.enterUids.contains(event.uid)) {
                    acc.chatUids.add(event.uid);
                }
            }
            
            return acc;
        }

        @Override
        public AnchorHotFeatureAgg getResult(AnchorHotAccumulator acc) {
            AnchorHotFeatureAgg result = new AnchorHotFeatureAgg();
            result.anchorId = acc.anchorId;
            result.enterUserCount = acc.enterUids.size();
            result.quitUserCount = acc.quitUids.size();
            result.giftUserCount = acc.giftUids.size();
            result.followUserCount = acc.followUids.size();
            result.chatUserCount = acc.chatUids.size();
            result.giftCoin = acc.totalGiftCoin;
            
            // 计算退房次均停留时长
            if (!acc.quitDurations.isEmpty()) {
                long totalDuration = 0;
                for (Long duration : acc.quitDurations) {
                    totalDuration += duration;
                }
                result.avgQuitDuration = (int) (totalDuration / acc.quitDurations.size());
            } else {
                result.avgQuitDuration = 0;
            }
            
            return result;
        }

        @Override
        public AnchorHotAccumulator merge(AnchorHotAccumulator a, AnchorHotAccumulator b) {
            a.enterUids.addAll(b.enterUids);
            a.quitUids.addAll(b.quitUids);
            a.quitDurations.addAll(b.quitDurations);
            a.giftUids.addAll(b.giftUids);
            a.followUids.addAll(b.followUids);
            a.chatUids.addAll(b.chatUids);
            a.totalGiftCoin += b.totalGiftCoin;
            return a;
        }
    }

    /**
     * 主播热度累加器
     */
    public static class AnchorHotAccumulator {
        public long anchorId;
        public Set<Long> enterUids = new HashSet<>();          // 进房用户uid集合（用于关联其他事件）
        public Set<Long> quitUids = new HashSet<>();           // 退房用户uid集合
        public List<Long> quitDurations = new ArrayList<>();   // 退房停留时长列表
        public Set<Long> giftUids = new HashSet<>();           // 送礼用户uid集合
        public Set<Long> followUids = new HashSet<>();         // 关注用户uid集合
        public Set<Long> chatUids = new HashSet<>();           // 聊天用户uid集合
        public int totalGiftCoin = 0;                          // 总礼物金额
    }

    /**
     * 主播热度特征聚合结果
     */
    public static class AnchorHotFeatureAgg {
        public long anchorId;
        public int enterUserCount;      // 进房人数
        public int quitUserCount;       // 退房人数
        public int avgQuitDuration;     // 退房次均停留时长（秒）
        public int giftCoin;            // 收礼金额
        public int giftUserCount;       // 收礼人数
        public int followUserCount;     // 关注人数
        public int chatUserCount;       // 聊天人数
    }
}
