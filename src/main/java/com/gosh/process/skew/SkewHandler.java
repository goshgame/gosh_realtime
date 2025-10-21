package com.gosh.process.skew;

import com.gosh.cons.SkewType;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 数据倾斜处理接口（用户可自定义实现）
 */
public interface SkewHandler<T> {
    /**
     * 处理数据倾斜
     * @param stream 输入数据流
     * @param skewType 倾斜类型（由SkewDetector检测得出）
     * @param keySelector Key选择器
     * @param baseParallelism 基础并行度
     * @return 处理后的平衡数据流
     */
    DataStream<T> handle(DataStream<T> stream, SkewType skewType, KeySelector<T, ?> keySelector, int baseParallelism);
}
