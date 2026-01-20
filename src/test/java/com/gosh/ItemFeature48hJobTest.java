package com.gosh.job;

import com.gosh.entity.RecFeature;
import com.gosh.job.AiTagParseCommon.PostInfoEvent;
import com.gosh.job.ItemFeatureCommon.ItemFeatureAccumulator;
import com.gosh.job.ItemFeature48hJob.Post48hCumulativeProcessFunction;
import com.gosh.job.UserFeatureCommon.UserFeatureEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

// The following imports are only available in JUnit 5.
// Make sure JUnit 5 is in your dependencies, or switch to JUnit 4 equivalents if needed.
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// If KeyedTwoInputOperatorTestHarness cannot be resolved, check your Flink test dependencies.
// See: https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/testing/
import org.apache.flink.streaming.util.KeyedTwoInputOperatorTestHarness;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ItemFeature48hJobTest {

    private KeyedTwoInputOperatorTestHarness<Long, UserFeatureEvent, PostInfoEvent, Tuple2<String, byte[]>> harness;
    private Post48hCumulativeProcessFunction function;

    // 定义一些常量，模拟 Job 中的静态变量
    private static final long WINDOW_SIZE_MS = 48 * 60 * 60 * 1000L;
    private static final long FLUSH_INTERVAL_MS = 60 * 1000L;
    private static final String REDIS_KEY_PREFIX = "rec:item_feature:{";
    private static final String REDIS_KEY_SUFFIX = "}:post48h";

    @BeforeEach
    void setUp() throws Exception {
        function = new Post48hCumulativeProcessFunction();
        harness = new KeyedTwoInputOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                event -> event.postId,
                event -> event.postId,
                org.apache.flink.api.common.typeinfo.Types.LONG);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void testCreationEventSetsStateAndTimers() throws Exception {
        long postId = 1L;
        long createdAtSec = 1678886400L; // 2023-03-15 12:00:00 UTC
        long createdAtMillis = createdAtSec * 1000;

        // 模拟 PostInfoEvent
        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;

        // 发送事件
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 验证 createdAtState
        assertEquals(createdAtMillis, function.createdAtState.value());

        // 验证 cleanupTimerState
        long expectedCleanupTime = createdAtMillis + WINDOW_SIZE_MS;
        assertEquals(expectedCleanupTime, function.cleanupTimerState.value());
        assertTrue(harness.getEventTimeTimers(postId).contains(expectedCleanupTime));

        // 验证 flushTimerState
        long currentProcessingTime = harness.getProcessingTime();
        long expectedFlushTime = currentProcessingTime + FLUSH_INTERVAL_MS;
        assertEquals(expectedFlushTime, function.flushTimerState.value());
        assertTrue(harness.getProcessingTimeTimers(postId).contains(expectedFlushTime));

        // 验证没有输出
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    void testInteractionEventUpdatesAccumulatorAndRegistersFlushTimer() throws Exception {
        long postId = 2L;
        long uid = 101L;
        long createdAtSec = 1678886400L; // 2023-03-15 12:00:00 UTC
        long createdAtMillis = createdAtSec * 1000;
        long eventTimeMillis = createdAtMillis + 10000; // 10秒后

        // 先发送创建事件
        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 模拟 UserFeatureEvent (expose)
        UserFeatureEvent exposeEvent = new UserFeatureEvent();
        exposeEvent.postId = postId;
        exposeEvent.uid = uid;
        exposeEvent.eventType = "expose";
        exposeEvent.timestamp = eventTimeMillis;
        exposeEvent.recToken = "rec_token_expose";

        // 发送事件
        harness.processElement1(new StreamRecord<>(exposeEvent, eventTimeMillis));

        // 验证 accumulatorState
        ItemFeatureAccumulator acc = function.accumulatorState.value();
        assertNotNull(acc);
        assertEquals(postId, acc.postId);
        assertEquals(1, acc.exposeHLL.cardinality());

        // 验证 flushTimerState (应该已经注册)
        assertNotNull(function.flushTimerState.value());

        // 验证没有输出
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    void testFlushTimerFiresAndEmitsResult() throws Exception {
        long postId = 3L;
        long uid = 102L;
        long createdAtSec = 1678886400L;
        long createdAtMillis = createdAtSec * 1000;
        long eventTimeMillis = createdAtMillis + 10000;

        // 1. 发送创建事件
        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 2. 发送交互事件
        UserFeatureEvent viewEvent = new UserFeatureEvent();
        viewEvent.postId = postId;
        viewEvent.uid = uid;
        viewEvent.eventType = "view";
        viewEvent.timestamp = eventTimeMillis;
        viewEvent.recToken = "rec_token_view";
        viewEvent.progressTime = 15.0f; // 触发 3s, 8s, 12s 观看
        viewEvent.standingTime = 10.0f; // 触发 5s, 10s 停留
        viewEvent.interaction = List.of(1); // 点赞
        harness.processElement1(new StreamRecord<>(viewEvent, eventTimeMillis));

        // 3. 模拟 ProcessingTime 推进，触发 flushTimer
        long flushTimerTimestamp = function.flushTimerState.value();
        harness.setProcessingTime(flushTimerTimestamp);
        harness.fireProcessingTime();

        // 4. 验证输出
        List<StreamRecord<Tuple2<String, byte[]>>> outputs = harness.getOutput();
        assertEquals(1, outputs.size());

        StreamRecord<Tuple2<String, byte[]>> output = outputs.get(0);
        assertEquals(REDIS_KEY_PREFIX + postId + REDIS_KEY_SUFFIX, output.getValue().f0);

        RecFeature.RecPostFeature feature = RecFeature.RecPostFeature.parseFrom(output.getValue().f1);
        assertEquals(postId, feature.getPostId());
        assertEquals(1, feature.getPost3SviewCnt48H());
        assertEquals(1, feature.getPost8SviewCnt48H());
        assertEquals(1, feature.getPost12SviewCnt48H());
        assertEquals(0, feature.getPost20SviewCnt48H()); // progressTime=15 < 20
        assertEquals(1, feature.getPost5SstandCnt48H());
        assertEquals(1, feature.getPost10SstandCnt48H());
        assertEquals(1, feature.getPostLikeCnt48H());
        assertEquals(0, feature.getPostFollowCnt48H());

        // 5. 验证新的 flushTimer 是否注册
        assertNotNull(function.flushTimerState.value());
        assertTrue(harness.getProcessingTimeTimers(postId).contains(flushTimerTimestamp + FLUSH_INTERVAL_MS));
    }

    @Test
    void testCleanupTimerFiresAndClearsState() throws Exception {
        long postId = 4L;
        long uid = 103L;
        long createdAtSec = 1678886400L;
        long createdAtMillis = createdAtSec * 1000;
        long eventTimeMillis = createdAtMillis + 10000;

        // 1. 发送创建事件
        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 2. 发送交互事件
        UserFeatureEvent exposeEvent = new UserFeatureEvent();
        exposeEvent.postId = postId;
        exposeEvent.uid = uid;
        exposeEvent.eventType = "expose";
        exposeEvent.timestamp = eventTimeMillis;
        exposeEvent.recToken = "rec_token_expose_final";
        harness.processElement1(new StreamRecord<>(exposeEvent, eventTimeMillis));

        // 3. 模拟 EventTime 推进，触发 cleanupTimer
        long cleanupTimerTimestamp = function.cleanupTimerState.value();
        harness.setOutput(new ArrayList<>()); // 清空之前的输出
        harness.processWatermark(new Watermark(cleanupTimerTimestamp)); // Watermark 推进到清理时间
        harness.fireEventTime();

        // 4. 验证输出 (最后一次 flush)
        List<StreamRecord<Tuple2<String, byte[]>>> outputs = harness.getOutput();
        assertEquals(1, outputs.size());
        StreamRecord<Tuple2<String, byte[]>> output = outputs.get(0);
        assertEquals(REDIS_KEY_PREFIX + postId + REDIS_KEY_SUFFIX, output.getValue().f0);
        RecFeature.RecPostFeature feature = RecFeature.RecPostFeature.parseFrom(output.getValue().f1);
        assertEquals(1, feature.getPostExpCnt48H()); // 曝光计数：exposeHLL 中有 1 个用户

        // 5. 验证所有状态是否已清除
        assertNull(function.createdAtState.value());
        assertNull(function.accumulatorState.value());
        assertNull(function.cleanupTimerState.value());
        assertNull(function.flushTimerState.value());

        // 验证没有剩余的定时器
        assertTrue(harness.getEventTimeTimers(postId).isEmpty());
        assertTrue(harness.getProcessingTimeTimers(postId).isEmpty());
    }

    @Test
    void testInteractionBeforeCreationEvent() throws Exception {
        long postId = 5L;
        long uid = 104L;
        long eventTimeMillis = 1678886400L * 1000; // 2023-03-15 12:00:00 UTC

        // 1. 发送交互事件 (在创建事件之前)
        UserFeatureEvent exposeEvent = new UserFeatureEvent();
        exposeEvent.postId = postId;
        exposeEvent.uid = uid;
        exposeEvent.eventType = "expose";
        exposeEvent.timestamp = eventTimeMillis;
        exposeEvent.recToken = "rec_token_early_expose";
        harness.processElement1(new StreamRecord<>(exposeEvent, eventTimeMillis));

        // 验证 accumulatorState 应该被更新
        ItemFeatureAccumulator acc = function.accumulatorState.value();
        assertNotNull(acc);
        assertEquals(postId, acc.postId);
        assertEquals(1, acc.exposeHLL.cardinality());

        // 验证 createdAtState 应该为 null
        assertNull(function.createdAtState.value());

        // 验证 flushTimerState 应该已注册
        assertNotNull(function.flushTimerState.value());

        // 2. 稍后发送创建事件
        long createdAtSec = eventTimeMillis / 1000 - 100; // 假设创建时间在交互事件之前100秒
        long createdAtMillis = createdAtSec * 1000;

        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 验证 createdAtState 应该被更新
        assertEquals(createdAtMillis, function.createdAtState.value());

        // 验证 cleanupTimerState 应该已注册
        assertNotNull(function.cleanupTimerState.value());
    }

    @Test
    void testInteractionEventOutsideWindow() throws Exception {
        long postId = 6L;
        long uid = 105L;
        long createdAtSec = 1678886400L; // 2023-03-15 12:00:00 UTC
        long createdAtMillis = createdAtSec * 1000;

        // 1. 发送创建事件
        PostInfoEvent creationEvent = new PostInfoEvent();
        creationEvent.postId = postId;
        creationEvent.createdAt = createdAtSec;
        harness.processElement2(new StreamRecord<>(creationEvent, createdAtMillis));

        // 2. 发送一个在 48h 窗口之外的交互事件
        long eventTimeOutsideWindow = createdAtMillis + WINDOW_SIZE_MS + 10000; // 48h + 10秒
        UserFeatureEvent exposeEvent = new UserFeatureEvent();
        exposeEvent.postId = postId;
        exposeEvent.uid = uid;
        exposeEvent.eventType = "expose";
        exposeEvent.timestamp = eventTimeOutsideWindow;
        exposeEvent.recToken = "rec_token_late_expose";
        harness.processElement1(new StreamRecord<>(exposeEvent, eventTimeOutsideWindow));

        // 验证 accumulatorState 应该没有更新 (仍然是空的，因为没有其他事件)
        ItemFeatureAccumulator acc = function.accumulatorState.value();
        assertNull(acc); // 或者如果之前有事件，则检查其值未改变

        // 验证 flushTimerState 应该没有重新注册 (如果之前没有交互事件)
        // 如果之前有交互事件，则 flushTimerState 应该保持不变，不会因为这个超窗事件而更新
        // 在这个测试场景中，由于这是第一个交互事件，且它在窗口外，所以不应该触发累加和定时器更新
        assertNotNull(function.flushTimerState.value()); // 仍然是由于 creationEvent 注册的
        // 确认没有新的 timer 被注册
        assertEquals(1, harness.getProcessingTimeTimers(postId).size()); // 只有 creationEvent 注册的那个

        // 验证没有输出
        assertTrue(harness.getOutput().isEmpty());
    }
}
