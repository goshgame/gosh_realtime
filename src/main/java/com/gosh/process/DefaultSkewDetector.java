package com.gosh.process;


import com.gosh.cons.SkewType;
import com.gosh.process.skew.SkewDetector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.configuration.Configuration;


/**
 * 默认倾斜检测实现（复用原DataSkewProcessor中的检测逻辑）
 */
public class DefaultSkewDetector<T, K> implements SkewDetector<T, K> {
    private final Configuration config;

    public DefaultSkewDetector(Configuration config) {
        this.config = config;
    }

    @Override
    public SkewType detect(DataStream<T> stream, KeySelector<T, K> keySelector) {
        // 复用DataSkewProcessor原有的detectSkewType逻辑
        return DataSkewProcessor.detectSkewType(stream, keySelector, config);
    }
}