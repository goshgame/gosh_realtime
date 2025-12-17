package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeature;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LiveUserPornLabelJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserPornLabelJob.class);
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:realpic";
    
    // 事件类型常量
    private static final String EVENT_LIVE_VIEW = "live_view";
    private static final String EVENT_EXIT_LIVEROOM = "exit_liveroom";
    
    // SessionWindow gap: 20秒
    private static final long SESSION_GAP_SECONDS = 20;
    
    // 观看时长阈值：15秒
    private static final long WATCH_DURATION_THRESHOLD_SECONDS = 15;
    
    // MySQL缓存更新间隔：60秒
    private static final long MYSQL_CACHE_UPDATE_INTERVAL_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Starting LiveUserPornLabelJob");
        LOG.info("Processing event_type=1 for user porn label features");
        LOG.info("Target events: live_view, exit_liveroom");
        LOG.info("SessionWindow gap: {} seconds", SESSION_GAP_SECONDS);
        LOG.info("Watch duration threshold: {} seconds", WATCH_DURATION_THRESHOLD_SECONDS);
        LOG.info("========================================");
        
        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        LOG.info("Job parallelism set to: {}", env.getParallelism());
        
        env.getConfig().setAutoWatermarkInterval(1000);

        // 第二步：创建Source，Kafka环境（topic=advertise）
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
            KafkaEnvUtil.loadProperties(), "advertise"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
            inputTopic,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 3.0 预过滤 - 只保留 event_type=1 的事件
        DataStream<String> filteredStream = kafkaSource
            .filter(EventFilterUtil.createFastEventTypeFilter(1))
            .name("Pre-filter Live Events (event_type=1)");

        // 3.1 解析 user_event_log 为 Live 用户-主播 事件
        SingleOutputStreamOperator<LiveUserAnchorEvent> eventStream = filteredStream
            .flatMap(new LiveEventParser())
            .name("Parse Live User-Anchor Events")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LiveUserAnchorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, recordTimestamp) -> event.eventTime)
                    .withIdleness(Duration.ofMinutes(1))
            );

        // 第四步：SessionWindow聚合：基于rec_token + anchor_id
        SingleOutputStreamOperator<SessionSummary> sessionStream = eventStream
            .keyBy(new KeySelector<LiveUserAnchorEvent, String>() {
                @Override
                public String getKey(LiveUserAnchorEvent value) throws Exception {
                    return value.recToken + "|" + value.anchorId;
                }
            })
            .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(SESSION_GAP_SECONDS)))
            .aggregate(new SessionAggregator(), new SessionWindowFunction())
            .name("Session Window Aggregation");

        // 第五步：判断并生成特征
        DataStream<Tuple2<String, byte[]>> featureStream = sessionStream
            .map(new PornLabelFeatureMapper())
            .filter(tuple -> tuple != null)
            .name("Generate Porn Label Feature");

        // 第六步：创建sink，Redis环境（TTL=3天，SET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(60 * 60 * 24 * 3); // 3天
        redisConfig.setCommand("SET");
        LOG.info("Redis config: TTL={} seconds (3 days), Command={}", 
            redisConfig.getTtl(), redisConfig.getCommand());
        
        RedisUtil.addRedisSink(
            featureStream,
            redisConfig,
            true,
            100
        );

        LOG.info("Job configured, starting execution...");
        String JOB_NAME = "Live User Porn Label Job";
        FlinkMonitorUtil.executeWithMonitor(env, JOB_NAME);
    }

    // 输入事件：从 user_event_log.event_data 解析
    public static class LiveUserAnchorEvent {
        public long uid;
        public long anchorId;
        public String eventType;
        public String recToken;  // rec_token用于session窗口
        public long watchDuration;  // 观看时长（浏览和退房事件的stay_duration）
        public long eventTime;  // 事件发生时间戳
    }

    // 解析器：解析 user_event_log.event 和 user_event_log.event_data
    public static class LiveEventParser implements FlatMapFunction<String, LiveUserAnchorEvent> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void flatMap(String value, Collector<LiveUserAnchorEvent> out) throws Exception {
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
                
                // 只处理 live_view 和 exit_liveroom 事件
                if (!EVENT_LIVE_VIEW.equals(event) && !EVENT_EXIT_LIVEROOM.equals(event)) {
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
                long anchorId = data.path("anchor_id").asLong(0);
                String recToken = data.path("rec_token").asText("");
                
                if (uid <= 0 || anchorId <= 0 || recToken == null || recToken.isEmpty()) {
                    return;
                }
                
                LiveUserAnchorEvent evt = new LiveUserAnchorEvent();
                evt.uid = uid;
                evt.anchorId = anchorId;
                evt.eventType = event;
                evt.recToken = recToken;
                
                // 从user_event_log中提取事件时间戳
                long eventTimeMillis = System.currentTimeMillis();
                if (userEventLog.has("timestamp")) {
                    eventTimeMillis = userEventLog.get("timestamp").asLong(eventTimeMillis);
                } 
                evt.eventTime = eventTimeMillis;
                
                // 提取观看时长（浏览和退房事件）
                if (EVENT_LIVE_VIEW.equals(event)) {
                    evt.watchDuration = data.path("watch_duration").asLong(0);
                } else if (EVENT_EXIT_LIVEROOM.equals(event)) {
                    evt.watchDuration = data.path("stay_duration").asLong(0);
                }
                
                out.collect(evt);
            } catch (Exception e) {
                // 静默处理异常
            }
        }
    }

    // ==================== SessionWindow聚合 ====================
    
    /**
     * SessionWindow输出摘要
     */
    public static class SessionSummary {
        public String recToken;
        public long uid;
        public long anchorId;
        public long totalWatchDuration;  // 总观看时长（秒）
        public long eventTime;          // 会话结束时间
    }
    
    /**
     * SessionWindow聚合器
     */
    public static class SessionAggregator implements AggregateFunction<LiveUserAnchorEvent, SessionAccumulator, SessionAccumulator> {
        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(LiveUserAnchorEvent event, SessionAccumulator acc) {
            // 初始化必填字段
            if (acc.recToken == null) {
                acc.recToken = event.recToken;
                acc.uid = event.uid;
                acc.anchorId = event.anchorId;
            }
            
            // 聚合观看时长
            acc.totalWatchDuration += event.watchDuration;
            
            return acc;
        }

        @Override
        public SessionAccumulator getResult(SessionAccumulator acc) {
            return acc;
        }

        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            a.totalWatchDuration += b.totalWatchDuration;
            return a;
        }
    }
    
    public static class SessionAccumulator {
        public String recToken;
        public long uid;
        public long anchorId;
        public long totalWatchDuration;
    }
    
    /**
     * SessionWindow处理函数：输出SessionSummary
     */
    public static class SessionWindowFunction extends ProcessWindowFunction<SessionAccumulator, SessionSummary, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<SessionAccumulator> elements, Collector<SessionSummary> out) throws Exception {
            SessionAccumulator acc = elements.iterator().next();
            if (acc == null || acc.recToken == null) {
                return;
            }
            
            SessionSummary summary = new SessionSummary();
            summary.recToken = acc.recToken;
            summary.uid = acc.uid;
            summary.anchorId = acc.anchorId;
            summary.totalWatchDuration = acc.totalWatchDuration;
            summary.eventTime = context.window().getEnd();
            
            out.collect(summary);
        }
    }

    // ==================== 特征生成 ====================
    
    /**
     * 色情主播缓存管理器
     */
    public static class PornAnchorCache {
        private static volatile Set<Long> pornAnchorSet = new HashSet<>();
        private static volatile long lastUpdateTime = 0;
        private static final Object LOCK = new Object();
        private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        
        static {
            // 启动定时更新任务
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    updateCache();
                } catch (Exception e) {
                    LOG.error("Failed to update porn anchor cache", e);
                }
            }, 0, MYSQL_CACHE_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
        
        public static void updateCache() {
            try {
                // 使用 gosh 数据库
                String sql = "SELECT uid FROM gosh.agency_member WHERE anchor_type IN (11, 15)";
                
                Set<Long> newSet = new HashSet<>();
                try (Connection conn = MySQLUtil.getConnection("db1")) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) {
                            long uid = rs.getLong("uid");
                            newSet.add(uid);
                        }
                    }
                }
                
                synchronized (LOCK) {
                    pornAnchorSet = newSet;
                    lastUpdateTime = System.currentTimeMillis();
                    LOG.info("Updated porn anchor cache, size={}, time={}", newSet.size(), lastUpdateTime);
                }
            } catch (Exception e) {
                LOG.error("Error updating porn anchor cache", e);
            }
        }
        
        public static boolean isPornAnchor(long anchorId) {
            synchronized (LOCK) {
                return pornAnchorSet.contains(anchorId);
            }
        }
    }
    
    /**
     * 特征生成器：判断是否写入色情标志特征
     */
    public static class PornLabelFeatureMapper extends RichMapFunction<SessionSummary, Tuple2<String, byte[]>> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化时更新一次缓存
            PornAnchorCache.updateCache();
        }
        
        @Override
        public Tuple2<String, byte[]> map(SessionSummary summary) throws Exception {
            // 判断：观看时长 > 15秒 且 主播是色情主播
            if (summary.totalWatchDuration > WATCH_DURATION_THRESHOLD_SECONDS 
                && PornAnchorCache.isPornAnchor(summary.anchorId)) {
                
                // 构建Protobuf特征
                byte[] featureBytes = RecFeature.RecUserFeature.newBuilder()
                    .setViewerLiveHise(1)
                    .build()
                    .toByteArray();
                
                String redisKey = PREFIX + summary.uid + SUFFIX;
                return new Tuple2<>(redisKey, featureBytes);
            }
            
            return null;
        }
    }

}
