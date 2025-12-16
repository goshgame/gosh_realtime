package com.gosh.job;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MixedTTLCleanupJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟输入数据
        DataStream<UserEvent> userEvents = env.fromElements(
                new UserEvent("user1", "login", System.currentTimeMillis()),
                new UserEvent("user1", "click", System.currentTimeMillis()),
                new UserEvent("user2", "login", System.currentTimeMillis()),
                new UserEvent("user1", "logout", System.currentTimeMillis())
        );

        // 处理数据并应用 TTL
        DataStream<UserSession> result = userEvents
                .keyBy(UserEvent::getUserId)
                .process(new UserSessionProcessor());

        result.print();
        env.execute("Mixed TTL Cleanup Example");
    }

    // 事件类
    public static class UserEvent {
        private String userId;
        private String eventType;
        private long timestamp;

        // 构造方法、getter、setter...
        public UserEvent(String userId, String eventType, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public String getUserId() { return userId; }
        public String getEventType() { return eventType; }
        public long getTimestamp() { return timestamp; }
    }

    // 结果类
    public static class UserSession {
        private String userId;
        private int eventCount;
        private long lastActivityTime;

        // 构造方法、getter、setter...
        public UserSession(String userId, int eventCount, long lastActivityTime) {
            this.userId = userId;
            this.eventCount = eventCount;
            this.lastActivityTime = lastActivityTime;
        }

        @Override
        public String toString() {
            return String.format("UserSession{userId='%s', eventCount=%d, lastActivityTime=%d}",
                    userId, eventCount, lastActivityTime);
        }
    }

    // 处理函数：演示混合 TTL 策略的使用
    public static class UserSessionProcessor extends KeyedProcessFunction<String, UserEvent, UserSession> {

        // 声明状态变量（不要在这里初始化！）
        private ValueState<Integer> eventCountState;
        private MapState<String, Long> lastEventTimeState;
        private ListState<String> eventHistoryState;

        @Override
        public void open(Configuration parameters) {
            // 1. 创建混合 TTL 配置
            StateTtlConfig mixedTTLConfig = createMixedTTLConfig();

            // 2. 为不同状态创建描述符并应用 TTL
            // a) ValueState 示例
            ValueStateDescriptor<Integer> eventCountDescriptor =
                    new ValueStateDescriptor<>("eventCountState", Types.INT);
            eventCountDescriptor.enableTimeToLive(mixedTTLConfig);

            // b) MapState 示例
            MapStateDescriptor<String, Long> lastEventTimeDescriptor =
                    new MapStateDescriptor<>("lastEventTimeState", Types.STRING, Types.LONG);
            lastEventTimeDescriptor.enableTimeToLive(mixedTTLConfig);

            // c) ListState 示例 - 使用不同的 TTL 配置
            StateTtlConfig shortTTLConfig = createShortTTLConfig();
            ListStateDescriptor<String> eventHistoryDescriptor =
                    new ListStateDescriptor<>("eventHistoryState", Types.STRING);
            eventHistoryDescriptor.enableTimeToLive(shortTTLConfig);

            // 3. 获取状态（在 open 方法中）
            eventCountState = getRuntimeContext().getState(eventCountDescriptor);
            lastEventTimeState = getRuntimeContext().getMapState(lastEventTimeDescriptor);
            eventHistoryState = getRuntimeContext().getListState(eventHistoryDescriptor);
        }

        @Override
        public void processElement(
                UserEvent event,
                Context ctx,
                Collector<UserSession> out
        ) throws Exception {

            // 注册第一个定时器（仅在第一次处理元素时）
            if (eventCountState.value() == null) {
                long cleanupInterval = 5 * 60 * 1000; // 5分钟
                long currentTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(currentTime + cleanupInterval);
            }

            // ========== 使用带 TTL 的状态 ==========

            // 1. 读取状态（会触发惰性清理）
            Integer currentCount = eventCountState.value();
            if (currentCount == null) {
                currentCount = 0;
            }

            // 2. 更新状态（会刷新 TTL）
            currentCount++;
            eventCountState.update(currentCount);

            // 3. 使用 MapState（同样有 TTL）
            lastEventTimeState.put(event.getEventType(), event.getTimestamp());

            // 4. 使用 ListState
            eventHistoryState.add(event.getEventType());

            // 5. 输出结果
            out.collect(new UserSession(event.getUserId(), currentCount, event.getTimestamp()));

            // ========== 手动触发清理检查 ==========
            performManualCleanupCheck();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserSession> out) throws Exception {
            // 定时器触发时的清理操作
            System.out.println("Timer fired for key: " + ctx.getCurrentKey());

            // 强制访问状态以触发惰性清理
            if (eventCountState.value() == null) {
                System.out.println("State has expired for key: " + ctx.getCurrentKey());
            }

            // 设置下一个定时器
            long nextTimer = timestamp + 5 * 60 * 1000; // 5分钟后
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }

        // ========== 辅助方法 ==========

        private StateTtlConfig createMixedTTLConfig() {
            return StateTtlConfig.newBuilder(Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    // 混合清理策略
                    .cleanupFullSnapshot()  // 策略1: 全量快照清理
                    // 策略2: 增量后台清理（针对频繁访问的状态）
                    .cleanupIncrementally(5,true)
                    // 策略3: RocksDB 压缩清理（针对存储量大的状态）
                    .cleanupInRocksdbCompactFilter(1000)  // 每处理1000条触发一次

                    .build();
        }
        
        private StateTtlConfig createShortTTLConfig() {
            // 为历史记录创建较短的 TTL
            return StateTtlConfig.newBuilder(Time.minutes(30))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
                    .build();
        }

        private void performManualCleanupCheck() {
            // 这里可以添加业务逻辑来检查状态是否需要清理
            try {
                // 示例：如果列表太大，手动清理旧记录
                Iterable<String> events = eventHistoryState.get();
                int count = 0;
                for (String event : events) {
                    count++;
                }
                if (count > 1000) {
                    System.out.println("Event history too large, consider manual cleanup");
                }
            } catch (Exception e) {
                // 忽略清理检查中的错误
            }
        }
    }
}