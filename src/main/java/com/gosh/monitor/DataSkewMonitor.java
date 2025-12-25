package com.gosh.monitor;

import com.gosh.cons.CommonConstants;
import com.gosh.util.ConfigurationUtil;
import com.gosh.util.LogsUtil;
import com.gosh.util.MessageUtil;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 数据倾斜监控器（独立实现数据倾斜监控逻辑）
 */
public class DataSkewMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(DataSkewMonitor.class);
    static {
        LogsUtil.setAllLogLevels();
    }
    public final ConcurrentHashMap<String, Long> keyCountMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService skewMonitorScheduler = Executors.newSingleThreadScheduledExecutor();

    // 倾斜监控配置
    private static long SKEW_CHECK_INTERVAL_MS;
    private static double SKEW_THRESHOLD_RATIO;
    private static long SKEW_MIN_COUNT_THRESHOLD;
    private final String jobName; // 修改为实例变量，避免多作业冲突
    private ScheduledFuture<?> monitorTask;

    static {
        try {
            Configuration flinkConfig = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.FLINK_DEFAULT_CONF);
            SKEW_CHECK_INTERVAL_MS = flinkConfig.getLong("flink.monitor.skew.check-interval-ms", 60000);
            SKEW_THRESHOLD_RATIO = flinkConfig.getDouble("flink.monitor.skew.threshold.ratio", 3.0);
            SKEW_MIN_COUNT_THRESHOLD = flinkConfig.getLong("flink.monitor.skew.threshold.min-count", 1000);
        } catch (Exception e) {
            LOG.warn("加载倾斜监控配置失败，使用默认值", e);
            SKEW_CHECK_INTERVAL_MS = 60000;
            SKEW_THRESHOLD_RATIO = 3.0;
            SKEW_MIN_COUNT_THRESHOLD = 1000;
        }
    }

    public DataSkewMonitor(String jobName) {
        this.jobName = jobName;
    }

    /**
     * 跟踪Key的出现次数
     */
    public void trackKeyCount(String operatorName, String key, long count) {
        if (key == null || count <= 0) {
            return;
        }
        String compositeKey = operatorName + ":" + key;
        keyCountMap.merge(compositeKey, count, Long::sum);
    }

    /**z x
     * 启动倾斜监控调度任务
     */
    public void startSkewMonitor() {
        LOG.warn("启动数据倾斜监控，检查间隔: {}ms，倾斜阈值比例: {}x，最小计数阈值: {}",
                SKEW_CHECK_INTERVAL_MS, SKEW_THRESHOLD_RATIO, SKEW_MIN_COUNT_THRESHOLD);
        if (monitorTask != null && !monitorTask.isCancelled()) {
            monitorTask.cancel(true);
        }

        monitorTask = skewMonitorScheduler.scheduleAtFixedRate(
                this::checkDataSkew,
                0, SKEW_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS
        );
    }

    /**
     * 检查数据倾斜并触发告警
     */
    private void checkDataSkew() {
        try {
            if (keyCountMap.isEmpty()) {
                LOG.warn("无Key计数数据，跳过倾斜检查");
                return;
            }
            ConcurrentHashMap<String, Long> tempMap = new ConcurrentHashMap<>(keyCountMap);
            keyCountMap.clear();

            // 按算子分组统计
            Map<String, Map<Object, Long>> operatorKeyCounts = new HashMap<>();
            for (Map.Entry<String, Long> entry : tempMap.entrySet()) {
                String[] parts = entry.getKey().split(":", 2);
                if (parts.length != 2) continue;
                String operatorName = parts[0];
                Object key = parts[1];
                operatorKeyCounts.computeIfAbsent(operatorName, k -> new HashMap<>())
                        .put(key, entry.getValue());
            }

            // 逐个算子检查倾斜
            for (Map.Entry<String, Map<Object, Long>> opEntry : operatorKeyCounts.entrySet()) {
                String operatorName = opEntry.getKey();
                Map<Object, Long> keyCounts = opEntry.getValue();

                long totalCount = keyCounts.values().stream().mapToLong(v -> v).sum();
                int keyCount = keyCounts.size();
                if (keyCount == 0 || totalCount < SKEW_MIN_COUNT_THRESHOLD) {
                    LOG.warn("算子[{}]数据量不足（总计数: {}，Key数量: {}），跳过倾斜检查",
                            operatorName, totalCount, keyCount);
                    continue;
                }
                double avgCount = (double) totalCount / keyCount;

                // 筛选倾斜Key
                List<Map.Entry<Object, Long>> skewedKeys = keyCounts.entrySet().stream()
                        .filter(e -> e.getValue() >= avgCount * SKEW_THRESHOLD_RATIO
                                && e.getValue() >= SKEW_MIN_COUNT_THRESHOLD)
                        .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                        .collect(Collectors.toList());

                if (!skewedKeys.isEmpty()) {
                    StringBuilder alertMsg = new StringBuilder();
                    alertMsg.append(String.format("Flink作业[%s]数据倾斜告警：\n", this.jobName));
                    alertMsg.append(String.format("算子名称: %s\n", operatorName));
                    alertMsg.append(String.format("总Key数: %d，总计数: %d，平均计数: %.2f\n",
                            keyCount, totalCount, avgCount));
                    alertMsg.append(String.format("倾斜阈值: 超过平均值%.1fx且计数≥%d\n",
                            SKEW_THRESHOLD_RATIO, SKEW_MIN_COUNT_THRESHOLD));
                    alertMsg.append("倾斜Key列表（前10名）：\n");

                    skewedKeys.stream().limit(10).forEach(e -> {
                        alertMsg.append(String.format("- Key: %s，计数: %d（平均值的%.1fx）\n",
                                e.getKey(), e.getValue(), e.getValue() / avgCount));
                    });

                    MessageUtil.sendLarkshuMsg(alertMsg.toString(), null);
                    LOG.warn("数据倾斜告警：{}", alertMsg.toString());
                }
            }

            // 重置计数
            keyCountMap.clear();
        } catch (Exception e) {
            LOG.error("数据倾斜检查失败", e);
        }
    }

    /**
     * 关闭倾斜监控资源
     */
    public void shutdown() {
        if (monitorTask != null && !monitorTask.isCancelled()) {
            monitorTask.cancel(true);
        }

        skewMonitorScheduler.shutdown();
        try {
            if (!skewMonitorScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                skewMonitorScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            skewMonitorScheduler.shutdownNow();
        }
        keyCountMap.clear();
        LOG.info("数据倾斜监控资源已关闭");
    }
}
