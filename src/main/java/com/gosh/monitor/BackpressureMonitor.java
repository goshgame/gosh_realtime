package com.gosh.monitor;

import com.gosh.cons.CommonConstants;
import com.gosh.util.ConfigurationUtil;
import com.gosh.util.MessageUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 反压监控器（基于FlinkMonitorUtil实现核心逻辑）
 */
public class BackpressureMonitor<T> extends RichMapFunction<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureMonitor.class);
    private static final ThreadLocal<Long> currentQueueLength = ThreadLocal.withInitial(() -> 0L);
    private transient ScheduledExecutorService backpressureScheduler;
    private transient Map<String, Integer> highBackpressureCount;

    // 反压监控配置
    private static long CHECK_INTERVAL_MS;
    private static int HIGH_BACKPRESSURE_THRESHOLD;
    private static long QUEUE_HIGH_THRESHOLD;
    private static Configuration flinkConfig;

    private final String jobName;
    private final String operatorName;
    private ScheduledFuture<?> monitorTask;
    private transient Gauge<Long> inputQueueLengthGauge;
    private transient org.apache.flink.metrics.Counter inputCount;
    private transient org.apache.flink.metrics.Counter outputCount;

    static {
        try {
            flinkConfig = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.FLINK_DEFAULT_CONF);
            CHECK_INTERVAL_MS = flinkConfig.getLong("flink.monitor.backpressure.check-interval-ms", 30000);
            HIGH_BACKPRESSURE_THRESHOLD = flinkConfig.getInteger("flink.monitor.backpressure.threshold.count", 3);
            QUEUE_HIGH_THRESHOLD = flinkConfig.getLong("flink.monitor.backpressure.threshold.queue-length", 10000);
        } catch (Exception e) {
            LOG.warn("加载反压监控配置失败，使用默认值", e);
            CHECK_INTERVAL_MS = 30000;
            HIGH_BACKPRESSURE_THRESHOLD = 3;
            QUEUE_HIGH_THRESHOLD = 10000;
        }
    }

    public BackpressureMonitor(String jobName, String operatorName) {
        this.jobName = jobName;
        this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
        // 初始化不可序列化的资源
        this.backpressureScheduler = Executors.newSingleThreadScheduledExecutor();
        this.highBackpressureCount = new ConcurrentHashMap<>();
        
        // 获取当前算子的MetricGroup，注册计数器
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        inputCount = metricGroup.counter("input_count");
        outputCount = metricGroup.counter("output_count");

        inputQueueLengthGauge = registerBackpressureMetric(getRuntimeContext().getMetricGroup());
        this.startBackpressureCheck();
        LOG.info("反压监控已启动，作业名称: {}", jobName);
    }

    @Override
    public T map(T value) {
        inputCount.inc();
        long queueLength = getInputQueueLength();
        currentQueueLength.set(queueLength);
        outputCount.inc();
        return value;
    }

    /**
     * 注册反压监控指标并启动定时检查任务
     */
    public Gauge<Long> registerBackpressureMetric(MetricGroup metricGroup) {
        return metricGroup.gauge("input_queue_length", BackpressureMonitor::getCurrentQueueLength);
    }
    /**
     * 启动反压检查调度任务
     */
    public void startBackpressureCheck() {
        // 启动前先取消已有任务
        if (monitorTask != null && !monitorTask.isCancelled()) {
            monitorTask.cancel(true);
        }
        monitorTask = backpressureScheduler.scheduleAtFixedRate(
                () -> checkBackpressure(),
                0, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS
        );
    }

    /**
     * 检查反压状态（基于输入队列长度）
     */
    private void checkBackpressure() {
        try {
            long currentQueueLength = getCurrentQueueLength();
            int currentCount = highBackpressureCount.getOrDefault(operatorName, 0);

//            LOG.warn("反压检查 - 算子[{}]：当前队列长度={}，阈值={}，累计超阈值次数={}/{}",
//                    operatorName, currentQueueLength, QUEUE_HIGH_THRESHOLD, currentCount, HIGH_BACKPRESSURE_THRESHOLD);

            if (currentQueueLength >= QUEUE_HIGH_THRESHOLD) {
                int updatedCount = currentCount + 1;
                highBackpressureCount.put(operatorName, updatedCount);
                LOG.warn("算子[{}]输入队列长度过高: {}（阈值: {}），累计超阈值次数: {}/{}",
                        operatorName, currentQueueLength, QUEUE_HIGH_THRESHOLD, updatedCount, HIGH_BACKPRESSURE_THRESHOLD);

                if (updatedCount >= HIGH_BACKPRESSURE_THRESHOLD) {
                    String alertMsg = String.format(
                            "Flink作业[%s]反压告警：算子[%s]连续%d次输入队列长度超过阈值（当前: %d），可能存在高反压！",
                            jobName, operatorName, HIGH_BACKPRESSURE_THRESHOLD, currentQueueLength
                    );
                    MessageUtil.sendLarkshuMsg(alertMsg, null);
                    highBackpressureCount.put(operatorName, 0); // 重置计数器
                    LOG.warn("算子[{}]已触发反压告警，计数器重置为0", operatorName);
                }
            } else if (currentCount > 0) {
                highBackpressureCount.put(operatorName, 0);
                LOG.warn("算子[{}]输入队列长度恢复正常: {}（阈值: {}），累计超阈值次数重置为0",
                        operatorName, currentQueueLength, QUEUE_HIGH_THRESHOLD);
            }
        } catch (Exception e) {
            LOG.error("反压检查失败", e);
        }
    }

    /**
     * 获取当前队列长度（供Gauge指标使用）
     */
    public static Long getCurrentQueueLength() {
        return currentQueueLength.get() == null ? 0 : currentQueueLength.get();
    }

    /**
     * 获取输入队列长度（基于实际指标）
     * 注意：这个方法提供了一个基础实现，实际使用时应根据具体算子类型进行优化
     * 例如：对于Kafka Source，可以通过consumer的metrics获取更准确的队列长度
     */
    private long getInputQueueLength() {
        // 正确计算队列长度：输入计数减去输出计数
        long queueEstimate = inputCount.getCount() - outputCount.getCount();
        // 确保返回非负值
        return Math.max(0, queueEstimate);
    }

    @Override
    public void close() {
        // 关闭监控资源（通常在作业结束时调用）
        shutdown();
        // 清理ThreadLocal，避免内存泄漏
        currentQueueLength.remove();
        LOG.warn("反压监控已停止，作业名称: {}", jobName);
    }
    /**
     * 关闭反压监控资源
     */
    public void shutdown() {
        if (monitorTask != null && !monitorTask.isCancelled()) {
            monitorTask.cancel(true);
        }
        backpressureScheduler.shutdown();
        try {
            if (!backpressureScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                backpressureScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            backpressureScheduler.shutdownNow();
        }
        highBackpressureCount.clear();
        LOG.warn("反压监控资源已关闭");
    }
}