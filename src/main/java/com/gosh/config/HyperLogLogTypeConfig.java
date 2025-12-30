package com.gosh.config;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.gosh.serializer.HyperLogLogSerializer;
import org.apache.flink.api.common.ExecutionConfig;

public class HyperLogLogTypeConfig {

    public static void registerHyperLogLogSerializer(ExecutionConfig config) {
        config.registerTypeWithKryoSerializer(HyperLogLog.class, HyperLogLogSerializer.class);
    }
}
