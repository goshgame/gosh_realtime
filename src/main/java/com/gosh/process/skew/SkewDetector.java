package com.gosh.process.skew;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.gosh.cons.SkewType;

/**
 * 数据倾斜检测接口（用户可自定义实现）
 */
public interface SkewDetector<T, K> {
    /**
     * 检测数据倾斜类型
     * @param stream 输入数据流
     * @param keySelector Key选择器
     * @return 倾斜类型（NONE/GLOBAL_SKEW/HOT_KEY/PARTITION_SKEW）
     */
    SkewType detect(DataStream<T> stream, KeySelector<T, K> keySelector);
}