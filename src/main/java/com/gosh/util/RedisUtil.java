package com.gosh.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.config.*;
import com.gosh.cons.CommonConstants;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
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
     * 添加Redis Sink，接收key-value对的Tuple数据流
     * @param stream 输入数据流（元素类型为Tuple2<String, byte[]>，key-value对）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param maxPendingOperations 最大并发操作数，默认10
     */
    public static void addRedisSink(
            DataStream<Tuple2<String, byte[]>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int... maxPendingOperations) {
        int maxOps = maxPendingOperations.length > 0 ? maxPendingOperations[0] : 10;
        stream.addSink(new RedisSink<>(config, async, batchSize, maxOps))
                .name("Redis Sink (Tuple2)");

        // 双写 kvrocks
        stream.addSink(new RedisSink<>(getKvRocksConfig(config), async, batchSize, maxOps))
                .name("KvRocks Sink (Tuple2)");
    }

    /**
     * 添加Redis Sink，接收key-value对的Tuple数据流（支持设置批处理时间间隔）
     * @param stream 输入数据流（元素类型为Tuple2<String, byte[]>，key-value对）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param maxPendingOperations 最大并发操作数，默认10
     * @param batchIntervalMs 批处理时间间隔（毫秒），默认5000毫秒
     */
    public static void addRedisSink(
            DataStream<Tuple2<String, byte[]>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int maxPendingOperations,
            long batchIntervalMs) {
        stream.addSink(new RedisSink<>(config, async, batchSize, maxPendingOperations, batchIntervalMs))
                .name("Redis Sink (Tuple2)");
        stream.addSink(new RedisSink<>(getKvRocksConfig(config), async, batchSize, maxPendingOperations, batchIntervalMs))
                .name("KvRocks Sink (Tuple2)");
    }

    /**
     * 简化版：使用默认配置和批量参数
     */
    public static void addRedisSink(DataStream<Tuple2<String, byte[]>> stream) {
        Properties props = loadProperties();
        stream.addSink(new RedisSink<>(props))
                .name("Redis Sink (Tuple2)");
    }

    /**
     * 添加Redis Hash Sink，接收key-field-value对的Tuple数据流，用于HSET等操作
     * @param stream 输入数据流（元素类型为Tuple3<String, String, byte[]>，key-field-value对）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param maxPendingOperations 最大并发操作数，默认10
     */
    public static void addRedisHashSink(
            DataStream<Tuple3<String, String, byte[]>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int... maxPendingOperations) {
        int maxOps = maxPendingOperations.length > 0 ? maxPendingOperations[0] : 10;
        stream.addSink(new RedisSink<>(config, async, batchSize, maxOps))
                .name("Redis Hash Sink (Tuple3)");
    }

    /**
     * 添加Redis Hash Sink，接收key-field-value对的Tuple数据流，用于HSET等操作（支持设置批处理时间间隔）
     * @param stream 输入数据流（元素类型为Tuple3<String, String, byte[]>，key-field-value对）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param maxPendingOperations 最大并发操作数
     * @param batchIntervalMs 批处理时间间隔（毫秒）
     */
    public static void addRedisHashSink(
            DataStream<Tuple3<String, String, byte[]>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int maxPendingOperations,
            long batchIntervalMs) {
        stream.addSink(new RedisSink<>(config, async, batchSize, maxPendingOperations, batchIntervalMs))
                .name("Redis Hash Sink (Tuple3)");
        stream.addSink(new RedisSink<>(getKvRocksConfig(config), async, batchSize, maxPendingOperations, batchIntervalMs))
                .name("KvRocks Hash Sink (Tuple3)");
    }

    /**
     * 添加Redis HashMap Sink - 用于批量HMSET操作
     */
    public static void addRedisHashMapSink(
            DataStream<Tuple2<String, Map<String, byte[]>>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int... maxPendingOperations) {
        int maxOps = maxPendingOperations.length > 0 ? maxPendingOperations[0] : 10;
        stream.addSink(new RedisSink<>(config, async, batchSize, maxOps))
                .name("Redis HashMap Sink (DEL_HMSET)");

        // 双写 kvrocks
        stream.addSink(new RedisSink<>(getKvRocksConfig(config), async, batchSize, maxOps))
                .name("KvRocks HashMap Sink (DEL_HMSET)");
    }

    /**
     * 添加Redis HashMap Sink - 用于批量HMSET操作（支持设置批处理时间间隔）
     * @param stream 输入数据流（元素类型为Tuple2<String, Map<String, byte[]>>，key-field-value map）
     * @param config Redis配置
     * @param async 是否异步写入
     * @param batchSize 批量写入大小
     * @param maxPendingOperations 最大并发操作数
     * @param batchIntervalMs 批处理时间间隔（毫秒）
     */
    public static void addRedisHashMapSink(
            DataStream<Tuple2<String, Map<String, byte[]>>> stream,
            RedisConfig config,
            boolean async,
            int batchSize,
            int maxPendingOperations,
            long batchIntervalMs) {
        stream.addSink(new RedisSink<>(config, async, batchSize, maxPendingOperations, batchIntervalMs))
                .name("Redis HashMap Sink (DEL_HMSET)");
    }

    private static RedisConfig getKvRocksConfig(RedisConfig config) {
        ObjectMapper mapper = new ObjectMapper();
        RedisConfig kvRocksConfig = null;
        try {
            // 深拷贝
            kvRocksConfig = mapper.readValue(
                    mapper.writeValueAsBytes(config),
                    RedisConfig.class
            );
            // 替换为 kvRocks 的 node，其他配置保持不变
            kvRocksConfig.setClusterNodes(List.of("kvrocks-prod-internal-nlb-f1afef52de25c89f.elb.ap-southeast-1.amazonaws.com:6666"));
            kvRocksConfig.setSsl(false);
            kvRocksConfig.setSslEnabled(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return kvRocksConfig;
    }
}