package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.RedisConfig;
import com.gosh.feature.RecFeature;
import com.gosh.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class LiveUserPornLabelJob {
    private static final Logger LOG = LoggerFactory.getLogger(LiveUserPornLabelJob.class);
    private static final String PREFIX = "rec:user_feature:{";
    private static final String SUFFIX = "}:livepic";
    
    // 事件类型常量
    private static final String EVENT_LIVE_VIEW = "live_view";
    private static final String EVENT_EXIT_LIVEROOM = "exit_liveroom";
    
    // SessionWindow gap: 20秒
    private static final long SESSION_GAP_SECONDS = 20;
    
    // 观看时长阈值：15秒
    private static final long WATCH_DURATION_THRESHOLD_SECONDS = 15;
    
    // MySQL缓存更新间隔：60秒（1分钟）
    private static final long MYSQL_CACHE_UPDATE_INTERVAL_SECONDS = 60;
    
    // Broadcast State 描述符
    private static final MapStateDescriptor<Void, Set<Long>> PORN_ANCHOR_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "porn-anchor-broadcast-state",
                    TypeInformation.of(Void.class),
                    TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<Long>>(){}));

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

        // 第四步：创建MySQL Source数据流，用于查询色情主播列表
        DataStream<Set<Long>> pornAnchorStream = env.addSource(
            new PornAnchorSourceFunction(),
            TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<Long>>(){})
        )
        .name("Porn Anchor MySQL Source");
        
        // 将MySQL数据流转换为BroadcastStream
        BroadcastStream<Set<Long>> pornAnchorBroadcastStream = pornAnchorStream
            .broadcast(PORN_ANCHOR_STATE_DESCRIPTOR);

        // 第五步：SessionWindow聚合：基于rec_token + anchor_id
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

        // 第六步：使用BroadcastProcessFunction判断并生成特征
        DataStream<Tuple2<String, byte[]>> featureStream = sessionStream
            .connect(pornAnchorBroadcastStream)
            .process(new PornLabelBroadcastProcessFunction())
            .filter(tuple -> tuple != null)
            .name("Generate Porn Label Feature");

        // 第七步：创建sink，Redis环境（TTL=3天，SET）
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        redisConfig.setTtl(60 * 60 * 24 * 3); // 3天
        redisConfig.setCommand("SET");
        LOG.info("Redis config: TTL={} seconds (3 days), Command={}", 
            redisConfig.getTtl(), redisConfig.getCommand());
        
        RedisUtil.addRedisSink(
            featureStream,
            redisConfig,
            true,
            10,
                10,
                1000
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
        private static volatile long PARSED_EVENT_COUNT = 0L;

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

                // 解析事件采样日志（每10万条打印一次）
                long c = ++PARSED_EVENT_COUNT;
                if (c <= 5 || c % 10000 == 0) {
                    LOG.info("[LiveEventParser] parsed={} sample: uid={}, anchorId={}, event={}, watchDuration={}",
                            c, evt.uid, evt.anchorId, evt.eventType, evt.watchDuration);
                }
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
        private static volatile long SESSION_COUNT = 0L;

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

            long c = ++SESSION_COUNT;
            // Session 聚合采样日志（前几条 + 每5万条）
            if (c <= 5 || c % 5000 == 0) {
                LOG.info("[SessionWindow] sessions={} sample: key={}, uid={}, anchorId={}, totalWatchDuration={}",
                        c, key, summary.uid, summary.anchorId, summary.totalWatchDuration);
            }

            out.collect(summary);
        }
    }

    // ==================== MySQL Source 和 Broadcast State ====================
    
    /**
     * 色情主播数据源：从MySQL定期查询并输出
     */
    public static class PornAnchorSourceFunction implements SourceFunction<Set<Long>> {
        private volatile boolean isRunning = true;
        
        @Override
        public void run(SourceContext<Set<Long>> ctx) throws Exception {
            LOG.info("PornAnchorSourceFunction started");
            
            while (isRunning) {
                try {
                    // 使用 MySQL 查询数据
                    String sql = "SELECT uid FROM gosh.agency_member WHERE anchor_type IN (11, 15)";
                    
                    Set<Long> anchorSet = new HashSet<>();
                    // 直接使用 MySQLUtil 查询（因为这是 SourceFunction，不在 Flink 算子链中）
                    MySQLUtil mysqlUtil = MySQLUtil.createNewInstance();
                    
                    try {
                        java.sql.Connection conn = null;
                        try {
                            conn = mysqlUtil.getConnectionInternal("db1");
                            try (java.sql.Statement stmt = conn.createStatement();
                                 java.sql.ResultSet rs = stmt.executeQuery(sql)) {
                                int count = 0;
                                while (rs.next()) {
                                    long uid = rs.getLong("uid");
                                    anchorSet.add(uid);
                                    count++;
                                }
                                LOG.info("Query executed successfully, found {} records", count);
                            }
                        } finally {
                            if (conn != null) {
                                try {
                                    conn.close();
                                } catch (Exception ignore) {
                                    // 忽略关闭异常
                                }
                            }
                        }
                    } finally {
                        try {
                            mysqlUtil.shutdownInternal();
                        } catch (Exception e) {
                            // 忽略关闭异常
                        }
                    }
                    
                    // 输出到 BroadcastStream
                    ctx.collect(anchorSet);
                    LOG.info("Porn anchor data updated and broadcasted, size={}", anchorSet.size());
                    
                    // 等待指定时间后再次查询
                    Thread.sleep(MYSQL_CACHE_UPDATE_INTERVAL_SECONDS * 1000);
                } catch (Exception e) {
                    // 打印完整的错误信息，包括堆栈
                    String errorMsg = e.getMessage();
                    String errorClass = e.getClass().getName();
                    LOG.error("Error querying porn anchor data. Error message: [{}], Error class: [{}]",
                            errorMsg != null ? errorMsg : "null", errorClass);

                    // 如果是特定异常，打印更多信息（单行）
                    if (e instanceof java.sql.SQLException) {
                        java.sql.SQLException sqlEx = (java.sql.SQLException) e;
                        LOG.error("SQLException details - SQLState: {}, VendorError: {}",
                                sqlEx.getSQLState(), sqlEx.getErrorCode());
                    }
                    
                    // 出错后等待一段时间再重试
                    Thread.sleep(MYSQL_CACHE_UPDATE_INTERVAL_SECONDS * 1000);
                }
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
            LOG.info("PornAnchorSourceFunction cancelled");
        }
    }
    
    /**
     * BroadcastProcessFunction：处理主数据流和广播流
     */
    public static class PornLabelBroadcastProcessFunction
            extends BroadcastProcessFunction<SessionSummary, Set<Long>, Tuple2<String, byte[]>> {

        private static volatile long FEATURE_EMIT_COUNT = 0L;
        private static volatile long NO_FEATURE_COUNT = 0L;
        
        @Override
        public void processElement(SessionSummary summary, 
                                   ReadOnlyContext ctx, 
                                   Collector<Tuple2<String, byte[]>> out) throws Exception {
            // 从 BroadcastState 读取色情主播集合
            ReadOnlyBroadcastState<Void, Set<Long>> broadcastState = 
                    ctx.getBroadcastState(PORN_ANCHOR_STATE_DESCRIPTOR);
            
            Set<Long> pornAnchorSet = broadcastState.get(null);
            
            // 如果广播状态还没有数据，跳过
            if (pornAnchorSet == null || pornAnchorSet.isEmpty()) {
                long c = ++NO_FEATURE_COUNT;
                if (c <= 5 || c % 50000 == 0) {
                    LOG.info("[PornLabel] skip because pornAnchorSet empty, totalSkipped={}", c);
                }
                return;
            }
            
            // 判断：观看时长 > 15秒 且 主播是色情主播
            if (summary.totalWatchDuration > WATCH_DURATION_THRESHOLD_SECONDS 
                && pornAnchorSet.contains(summary.anchorId)) {

                // 构建Protobuf特征
                byte[] featureBytes = RecFeature.RecLiveUserFeature.newBuilder()
                    .setViewerLiveHise(1)
                    .build()
                    .toByteArray();
                
                String redisKey = PREFIX + summary.uid + SUFFIX;

                long c = ++FEATURE_EMIT_COUNT;
                if (c <= 5 || c % 10000 == 0) {
                    LOG.info("[PornLabel] emit feature={}: uid={}, anchorId={}, totalWatchDuration={}, redisKey={}",
                            c, summary.uid, summary.anchorId, summary.totalWatchDuration, redisKey);
                }

                out.collect(new Tuple2<>(redisKey, featureBytes));
            } else {
                long c = ++NO_FEATURE_COUNT;
                if (c <= 5 || c % 100000 == 0) {
                    LOG.info("[PornLabel] no feature, totalNoFeature={}, uid={}, anchorId={}, duration={}, inPornSet={}",
                            c, summary.uid, summary.anchorId, summary.totalWatchDuration,
                            pornAnchorSet.contains(summary.anchorId));
                }
            }
        }
        
        @Override
        public void processBroadcastElement(Set<Long> anchorSet,
                                           Context ctx,
                                           Collector<Tuple2<String, byte[]>> out) throws Exception {
            // 更新 BroadcastState
            BroadcastState<Void, Set<Long>> broadcastState = 
                    ctx.getBroadcastState(PORN_ANCHOR_STATE_DESCRIPTOR);
            
            broadcastState.put(null, anchorSet);
            LOG.info("BroadcastState updated with {} porn anchors", anchorSet.size());
        }
    }

}
