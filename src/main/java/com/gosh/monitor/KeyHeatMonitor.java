package com.gosh.monitor;

import com.gosh.util.ConfigurationUtil;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.gosh.cons.CommonConstants;

/**
 * Key热度监控器，用于检测数据流中的热点Key
 * 基于滑动窗口统计Key出现频率，支持配置采样率和窗口大小
 */
public class KeyHeatMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(KeyHeatMonitor.class);
    private static final KeyHeatMonitor INSTANCE = new KeyHeatMonitor();

    private static final Configuration config = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.FLINK_SKEW_CONF);
    // 滑动窗口大小(毫秒)，默认60秒
    private static final long WINDOW_SIZE = config.getLong("skew.monitor.window.size", 60_000);
    // 采样率(0.0-1.0)，默认100%采样
    private static final double SAMPLE_RATE = config.getDouble("skew.monitor.sample.rate", 1.0);
    // 最小样本量，低于此值不触发热点判断
    private static final int MIN_SAMPLE_SIZE = config.getInteger("skew.monitor.min.sample", 100);

    // Key访问计数容器
    private final Map<String, AtomicLong> keyCounter = new ConcurrentHashMap<>();
    // 总访问计数
    private final AtomicLong totalCounter = new AtomicLong(0);
    // 定时清理线程池
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private KeyHeatMonitor() {
        // 初始化滑动窗口清理任务
        scheduler.scheduleAtFixedRate(this::resetWindow, WINDOW_SIZE, WINDOW_SIZE, TimeUnit.MILLISECONDS);
        LOG.warn("KeyHeatMonitor初始化完成，窗口大小: {}ms, 采样率: {}%, 最小样本量: {}",
                WINDOW_SIZE, (int)(SAMPLE_RATE * 100), MIN_SAMPLE_SIZE);
    }

    /**
     * 获取单例实例
     */
    public static KeyHeatMonitor getInstance() {
        return INSTANCE;
    }

    /**
     * 记录Key访问（带采样）
     * @param key 要监控的Key
     */
    public void recordKeyAccess(String key) {
        // 采样逻辑
        if (SAMPLE_RATE < 1.0 && Math.random() > SAMPLE_RATE) {
            return;
        }

        keyCounter.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
        totalCounter.incrementAndGet();
    }

    /**
     * 判断Key是否为热点Key
     * @param key 待判断的Key
     * @param threshold 热点阈值(0.0-1.0)，表示Key占比超过该值则判定为热点
     * @return 是否为热点Key
     */
    public static boolean isHotKey(String key, double threshold) {
        if (key == null || threshold <= 0 || threshold > 1.0) {
            return false;
        }

        KeyHeatMonitor monitor = getInstance();
        long total = monitor.totalCounter.get();

        // 样本量不足时不判断为热点
        if (total < MIN_SAMPLE_SIZE) {
            return false;
        }

        long keyCount = monitor.keyCounter.getOrDefault(key, new AtomicLong(0)).get();
        double keyRatio = (double) keyCount / total;

        // 日志输出热点判断过程
        if (keyRatio >= threshold) {
            LOG.warn("检测到热点Key: {}, 占比: {:.2f}%, 阈值: {:.2f}%",
                    key, keyRatio * 100, threshold * 100);
            return true;
        }

        return false;
    }

    /**
     * 重置滑动窗口计数器
     */
    private void resetWindow() {
        long total = totalCounter.getAndSet(0);
        keyCounter.clear();
        LOG.debug("滑动窗口已重置，上一窗口总访问量: {}", total);
    }

    /**
     * 关闭监控器（用于测试环境）
     */
    public void shutdown() {
        scheduler.shutdown();
    }
}
