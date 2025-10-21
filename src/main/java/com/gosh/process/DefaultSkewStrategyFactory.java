package com.gosh.process;


import com.gosh.process.skew.SkewDetector;
import com.gosh.process.skew.SkewHandler;
import com.gosh.process.skew.SkewStrategyFactory;
import org.apache.flink.configuration.Configuration;

/**
 * 默认策略工厂（提供默认检测+处理组合）
 */
public class DefaultSkewStrategyFactory<T, K> implements SkewStrategyFactory<T, K> {
    private final Configuration config;

    public DefaultSkewStrategyFactory(Configuration config) {
        this.config = config;
    }

    @Override
    public SkewDetector<T, K> getDetector() {
        return new DefaultSkewDetector<>(config);
    }

    @Override
    public SkewHandler<T> getHandler() {
        return new DefaultSkewHandler<>();
    }
}

