package com.gosh.config;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.Map;
import java.util.HashMap;

/**
 * Flink Redis Sink 增强版（支持通用protobuf解析）
 * 支持异步操作、批量操作和动态protobuf类型
 * 支持 DEL 和 HSET 操作
 */
public class RedisSink<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConfig config;
    private final boolean async;
    private final int batchSize;
    private transient RedisCommands<String, Tuple2<String, byte[]>> redisCommands;
    private transient RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> redisClusterCommands;
    private transient AtomicInteger pendingOperations;
    private transient RedisConnectionManager connectionManager;
    private transient boolean isClusterMode;

    public RedisSink(RedisConfig config, boolean async, int batchSize) {
        this.config = config;
        this.async = async;
        this.batchSize = batchSize;
    }

    public RedisSink(Properties props) {
        this(RedisConfig.fromProperties(props), true, 100);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.connectionManager = RedisConnectionManager.getInstance(config);
        this.isClusterMode = config.isClusterMode();
        if(this.isClusterMode){
            this.redisClusterCommands = isClusterMode ? connectionManager.getRedisClusterCommands() : null;
        }else {
            this.redisCommands = connectionManager.getRedisCommands();
        }
        this.pendingOperations = new AtomicInteger(0);

        LOG.info("Redis Sink opened with config: {}, async: {}, batchSize: {}", config, async, batchSize);
    }

    private boolean isRunning() {
        try {
            if (isClusterMode) {
                return redisClusterCommands != null;
            } else {
                return redisCommands != null;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(T value, Context context) throws Exception {
        // 检查连接是否已关闭
        if (connectionManager == null || !isRunning()) {
            LOG.warn("连接已关闭，跳过处理任务");
            return;
        }

        String key = null;
        String field = null;
        byte[] valueBytes = null;
        Map<String, byte[]> valueMap = null;

        if (value instanceof Tuple2) {
            Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
            key = tuple.f0.toString();
            if (tuple.f1 instanceof byte[]) {
                valueBytes = (byte[]) tuple.f1;
            } else if (tuple.f1 instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, byte[]> map = (Map<String, byte[]>) tuple.f1;
                valueMap = map;
            }
        } else if (value instanceof Tuple3) {
            Tuple3<?, ?, ?> tuple = (Tuple3<?, ?, ?>) value;
            key = tuple.f0.toString();
            field = tuple.f1.toString();
            valueBytes = (byte[]) tuple.f2;
        }

        if (key == null) {
            LOG.error("Key is required");
            return;
        }

        String command = config.getCommand().toUpperCase();
        if (isClusterMode) {
            executeClusterCommand(command, key, field, valueBytes, valueMap);
        } else {
            executeSingleCommand(command, key, field, valueBytes, valueMap);
        }
    }

    private void executeSingleCommand(String command, String key, String field, byte[] valueBytes, Map<String, byte[]> valueMap) {
        try {
            switch (command) {
                case "SET":
                    if (valueBytes == null) {
                        LOG.error("Value is required for SET command");
                        return;
                    }
                    LOG.debug("SET to Redis - key: {}", key);
                    redisCommands.set(key, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                case "HSET":
                    if (field == null || valueBytes == null) {
                        LOG.error("Field and value are required for HSET command");
                        return;
                    }
                    LOG.debug("HSET to Redis - key: {}, field: {}", key, field);
                    redisCommands.hset(key, field, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                case "DEL_HSET":
                    if (field == null || valueBytes == null) {
                        LOG.error("Field and value are required for DEL_HSET command");
                        return;
                    }
                    LOG.debug("DEL then HSET to Redis - key: {}, field: {}", key, field);
                    redisCommands.del(key);
                    redisCommands.hset(key, field, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                case "DEL_HMSET":
                    if (valueMap == null || valueMap.isEmpty()) {
                        LOG.error("Value map is required for DEL_HMSET command");
                        return;
                    }
                    LOG.debug("DEL then HMSET to Redis - key: {}, fields: {}", key, valueMap.size());
                    redisCommands.del(key);
                    Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                    for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                        wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                    }
                    redisCommands.hmset(key, wrappedMap);
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                case "LPUSH":
                case "RPUSH":
                    if (valueBytes == null) {
                        LOG.error("Value is required for " + command + " command");
                        return;
                    }
                    LOG.debug("{} to Redis - key: {}", command, key);
                    Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                    if (command.equals("LPUSH")) {
                        redisCommands.lpush(key, tuple);
                    } else {
                        redisCommands.rpush(key, tuple);
                    }
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                case "SADD":
                    if (valueBytes == null) {
                        LOG.error("Value is required for SADD command");
                        return;
                    }
                    LOG.debug("SADD to Redis - key: {}", key);
                    redisCommands.sadd(key, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                    break;
                default:
                    LOG.error("Unsupported Redis command: {}", command);
            }
        } catch (Exception e) {
            LOG.error("Failed to execute Redis command: " + command, e);
        }
    }

    private void executeClusterCommand(String command, String key, String field, byte[] valueBytes, Map<String, byte[]> valueMap) {
        try {
            switch (command) {
                case "SET":
                    if (valueBytes == null) {
                        LOG.error("Value is required for SET command");
                        return;
                    }
                    LOG.debug("SET to Redis Cluster - key: {}", key);
                    redisClusterCommands.set(key, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                case "HSET":
                    if (field == null || valueBytes == null) {
                        LOG.error("Field and value are required for HSET command");
                        return;
                    }
                    LOG.debug("HSET to Redis Cluster - key: {}, field: {}", key, field);
                    redisClusterCommands.hset(key, field, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                case "DEL_HSET":
                    if (field == null || valueBytes == null) {
                        LOG.error("Field and value are required for DEL_HSET command");
                        return;
                    }
                    LOG.debug("DEL then HSET to Redis Cluster - key: {}, field: {}", key, field);
                    redisClusterCommands.del(key);
                    redisClusterCommands.hset(key, field, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                case "DEL_HMSET":
                    if (valueMap == null || valueMap.isEmpty()) {
                        LOG.error("Value map is required for DEL_HMSET command");
                        return;
                    }
                    LOG.debug("DEL then HMSET to Redis Cluster - key: {}, fields: {}", key, valueMap.size());
                    redisClusterCommands.del(key);
                    Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                    for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                        wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                    }
                    redisClusterCommands.hmset(key, wrappedMap);
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                case "LPUSH":
                case "RPUSH":
                    if (valueBytes == null) {
                        LOG.error("Value is required for " + command + " command");
                        return;
                    }
                    LOG.debug("{} to Redis Cluster - key: {}", command, key);
                    Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                    if (command.equals("LPUSH")) {
                        redisClusterCommands.lpush(key, tuple);
                    } else {
                        redisClusterCommands.rpush(key, tuple);
                    }
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                case "SADD":
                    if (valueBytes == null) {
                        LOG.error("Value is required for SADD command");
                        return;
                    }
                    LOG.debug("SADD to Redis Cluster - key: {}", key);
                    redisClusterCommands.sadd(key, new Tuple2<>("", valueBytes));
                    if (config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                    break;
                default:
                    LOG.error("Unsupported Redis command: {}", command);
            }
        } catch (Exception e) {
            LOG.error("Failed to execute Redis Cluster command: " + command, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (pendingOperations.get() > 0) {
            LOG.info("等待 {} 个未完成的操作完成...", pendingOperations.get());
            long timeout = 30000;
            long interval = 100;
            long waited = 0;
            while (pendingOperations.get() > 0 && waited < timeout) {
                Thread.sleep(interval);
                waited += interval;
            }
        }

        super.close();
        connectionManager.shutdown();
        LOG.info("Redis Sink closed");
    }

    private static class DefaultFieldExtractor<M extends Message> implements Function<M, String>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public String apply(M m) {
            return String.valueOf(m.hashCode());
        }
    }
}