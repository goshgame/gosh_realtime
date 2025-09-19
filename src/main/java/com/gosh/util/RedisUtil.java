package com.gosh.util;

import com.google.protobuf.Message;
import com.gosh.config.*;
import com.gosh.cons.CommonConstants;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;
import java.util.function.Function;

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
     * 添加Redis Sink并指定protobuf解析器
     * @param stream 输入数据流（元素类型为byte[]）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param protoClass protobuf解析器（如RecUserFeatureOuterClass.RecUserFeature.PARSER）
     * @param keyExtractor 从protobuf消息提取Redis key的函数
     * @param <M> protobuf消息类型
     */
    public static <M extends Message> void addRedisSink(
            DataStream<byte[]> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            Class<M> protoClass, // 改为传入Class<M>
            Function<M, String> keyExtractor) {
        stream.addSink(new RedisSink<>(config, async, batchSize, protoClass, keyExtractor))
                .name("Redis Sink");
    }

    /**
     * 简化版：使用默认配置和批量参数
     */
    public static <M extends Message> void addRedisSink(
            DataStream<byte[]> stream,
            Class<M> protoClass, // 传入Class<M>
            Function<M, String> keyExtractor) {
        Properties props = loadProperties();
        stream.addSink(new RedisSink<>(props, protoClass, keyExtractor))
                .name("Redis Sink");
    }

    // 原有方法保持兼容（如需保留）
    @Deprecated
    public static void addRedisSink(DataStream stream) {
        Properties props = loadProperties();
        // 注意：原有方法未指定protobuf参数，此处需根据实际情况处理（如抛出异常或使用默认类型）
        throw new UnsupportedOperationException("请使用带protobuf参数的addRedisSink方法");
    }

    @Deprecated
    public static void addRedisSink(DataStream stream, RedisConfig config) {
        throw new UnsupportedOperationException("请使用带protobuf参数的addRedisSink方法");
    }

    @Deprecated
    public static void addRedisSink(DataStream stream, RedisConfig config, boolean async, int batchSize) {
        throw new UnsupportedOperationException("请使用带protobuf参数的addRedisSink方法");
    }
}