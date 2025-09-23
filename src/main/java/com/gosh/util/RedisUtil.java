package com.gosh.util;

import com.gosh.config.*;
import com.gosh.cons.CommonConstants;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;

public class RedisUtil {

    public static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = RedisUtil.class.getClassLoader().getResourceAsStream(CommonConstants.REDIS_DEFAULT_CONF)) {
            if (input == null) {
                throw new RuntimeException("无法找到配置文件: " + CommonConstants.REDIS_DEFAULT_CONF);
            }
            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败: " + CommonConstants.REDIS_DEFAULT_CONF, e);
        }
        return props;
    }

    // 原有RedisSource方法保持不变...
    public static DataStream createRedisSource(StreamExecutionEnvironment env) {
        Properties props = loadProperties();
        return env.addSource(new RedisSource(props)).name("Redis Source");
    }

    public static Tuple2<StreamExecutionEnvironment, DataStream> createRedisSourceWithEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = loadProperties();
        DataStream dataStream = env.addSource(new RedisSource(props)).name("Redis Source");
        return Tuple2.of(env, dataStream);
    }

    public static DataStream createRedisSource(StreamExecutionEnvironment env, RedisConfig config) {
        return env.addSource(new RedisSource(config)).name("Redis Source");
    }

    /**
     * 调整后：添加Redis Sink，接收key-value对的Tuple数据流
     * @param stream 输入数据流（元素类型为Tuple2<String, String>，key-value对）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     */
    public static void addRedisSink(
            DataStream<Tuple2<String, byte[]>> stream,
            RedisConfig config,
            boolean async,
            int batchSize) {
        stream.addSink(new RedisSink<>(config, async, batchSize))
                .name("Redis Sink (Tuple2)");
    }

    /**
     * 简化版：使用默认配置和批量参数
     */
    public static void addRedisSink(
            DataStream<Tuple2<String, byte[]>> stream) {
        Properties props = loadProperties();
        stream.addSink(new RedisSink<>(props))
                .name("Redis Sink (Tuple2)");
    }
}