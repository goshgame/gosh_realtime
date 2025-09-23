package com.gosh.util;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.util.concurrent.TimeUnit;

public class TimerUtil {
    private static final Timer SHARED_TIMER = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 512);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            SHARED_TIMER.stop();
        }));
    }

    public static Timer getSharedTimer() {
        return SHARED_TIMER;
    }
} 