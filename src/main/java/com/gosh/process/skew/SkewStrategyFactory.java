package com.gosh.process.skew;

/**
 * 倾斜策略工厂接口（用于整合检测与处理逻辑）
 */
public interface SkewStrategyFactory<T, K> {
    SkewDetector<T, K> getDetector();
    SkewHandler<T> getHandler();
}
