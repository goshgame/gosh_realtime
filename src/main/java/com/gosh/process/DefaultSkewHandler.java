package com.gosh.process;

import com.gosh.cons.SkewType;
import com.gosh.process.skew.SkewHandler;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * 默认倾斜处理实现（复用原DataSkewProcessor中的处理逻辑）
 */
public class DefaultSkewHandler<T> implements SkewHandler<T> {
    @Override
    public DataStream<T> handle(DataStream<T> stream, SkewType skewType, KeySelector<T, ?> keySelector, int baseParallelism) {
        // 复用DataSkewProcessor原有的倾斜处理逻辑
        switch (skewType) {
            case GLOBAL_SKEW:
                return DataSkewProcessor.adjustParallelism(stream, baseParallelism);
            case HOT_KEY:
                return DataSkewProcessor.rebalanceHotKey(stream, keySelector);
            case PARTITION_SKEW:
                return DataSkewProcessor.rebalancePartitions(stream);
            default:
                return stream;
        }
    }
}