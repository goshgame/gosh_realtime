package com.gosh.util;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TimerUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TimerUtil.class);
    private static final Timer SHARED_TIMER;

    static {
        LOG.info("Initializing shared HashedWheelTimer");
        SHARED_TIMER = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 512);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping shared HashedWheelTimer");
            SHARED_TIMER.stop();
        }));
    }

    private TimerUtil() {
        // 私有构造函数防止实例化
    }

    public static Timer getSharedTimer() {
        return SHARED_TIMER;
    }

    public static void scheduleWithRetry(Runnable task, long delay, TimeUnit unit) {
        SHARED_TIMER.newTimeout(timeout -> {
            try {
                task.run();
            } catch (Exception e) {
                LOG.error("Task execution failed", e);
            }
        }, delay, unit);
    }

    public static void scheduleWithRetry(Runnable task, long delay, TimeUnit unit, int maxRetries) {
        scheduleWithRetryInternal(task, delay, unit, 0, maxRetries);
    }

    private static void scheduleWithRetryInternal(Runnable task, long delay, TimeUnit unit, int currentRetry, int maxRetries) {
        SHARED_TIMER.newTimeout(timeout -> {
            try {
                task.run();
            } catch (Exception e) {
                if (currentRetry < maxRetries) {
                    long nextDelay = (long) (delay * Math.pow(2, currentRetry));
                    LOG.warn("Task failed, retry {}/{} in {} {}", currentRetry + 1, maxRetries, nextDelay, unit);
                    scheduleWithRetryInternal(task, nextDelay, unit, currentRetry + 1, maxRetries);
                } else {
                    LOG.error("Task failed after {} retries", maxRetries, e);
                }
            }
        }, delay, unit);
    }
} 