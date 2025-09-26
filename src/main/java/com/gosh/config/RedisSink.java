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
        if (connectionManager == null || !isRunning()) {
            LOG.warn("连接已关闭，跳过处理任务");
            return;
        }

        if (value == null) {
            LOG.warn("invoke: 接收为空值(null)，跳过处理");
            return;
        }

        // 支持 Tuple2(key, value) 和 Tuple3(key, field, value) 格式
        final String key;
        final String field;
        final byte[] valueBytes;

        if (value instanceof Tuple3) {
            Tuple3<String, String, byte[]> tuple = (Tuple3<String, String, byte[]>) value;
            key = tuple.f0;
            field = tuple.f1;
            valueBytes = tuple.f2;
        } else {
            Tuple2<String, byte[]> tuple = (Tuple2<String, byte[]>) value;
            key = tuple.f0;
            field = null;
            valueBytes = tuple.f1;
        }

        if (async) {
            CompletableFuture<Void> future;
            if (isClusterMode) {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeClusterAsync(commands -> {
                            executeCommand(commands, key, field, valueBytes);
                            return null;
                        }),
                        3
                );
            } else {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeAsync(commands -> {
                            executeCommand(commands, key, field, valueBytes);
                            return null;
                        }),
                        3
                );
            }

            if(future != null && config.getTtl() > 0 && !config.getCommand().equalsIgnoreCase("DEL")) {
                future.whenComplete((r, t) -> {
                    try {
                        CompletableFuture<Void> ttlFuture = isClusterMode ?
                                connectionManager.executeClusterAsync(commands -> {
                                    commands.expire(key, config.getTtl());
                                    return null;
                                }) :
                                connectionManager.executeAsync(commands -> {
                                    commands.expire(key, config.getTtl());
                                    return null;
                                });

                        ttlFuture.exceptionally(ex -> {
                            LOG.error("设置TTL失败, key=" + key, ex);
                            return null;
                        });
                    } catch (Exception e) {
                        LOG.error("Error setting TTL in Redis: {}", e.getMessage(), e);
                    }
                });
            }

            if (pendingOperations.incrementAndGet() >= batchSize) {
                future.get();
                pendingOperations.set(0);
            }
        } else {
            if(isClusterMode){
                executeCommand(redisClusterCommands, key, field, valueBytes);
                if (config.getTtl() > 0 && !config.getCommand().equalsIgnoreCase("DEL")) {
                    try {
                        redisClusterCommands.expire(key, config.getTtl());
                        LOG.debug("同步设置集群模式TTL成功, key={}, ttl={}", key, config.getTtl());
                    } catch (Exception e) {
                        LOG.error("同步设置集群模式TTL失败, key=" + key, e);
                    }
                }
            } else {
                executeCommand(redisCommands, key, field, valueBytes);
                if (config.getTtl() > 0 && !config.getCommand().equalsIgnoreCase("DEL")) {
                    try {
                        redisCommands.expire(key, config.getTtl());
                        LOG.debug("同步设置单机模式TTL成功, key={}, ttl={}", key, config.getTtl());
                    } catch (Exception e) {
                        LOG.error("同步设置单机模式TTL失败, key=" + key, e);
                    }
                }
            }
        }
    }

    private void executeCommand(RedisCommands<String, Tuple2<String, byte[]>> commands, String key, String field, byte[] value) {
        try {
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            Tuple2<String, byte[]> tuple = new Tuple2<>("", value);
            switch (command.toUpperCase()) {
                case "SET":
                    LOG.debug("Writing to Redis - key: {}", key);
                    commands.set(key, tuple);
                    break;
                case "DEL":
                    LOG.debug("Deleting from Redis - key: {}", key);
                    commands.del(key);
                    break;
                case "HSET":
                    if (field == null) {
                        LOG.error("Field is required for HSET command");
                        return;
                    }
                    LOG.debug("HSET to Redis - key: {}, field: {}", key, field);
                    commands.hset(key, field, tuple);
                    break;
                case "DEL_HSET":
                    if (field == null) {
                        LOG.error("Field is required for DEL_HSET command");
                        return;
                    }
                    LOG.debug("DEL then HSET to Redis - key: {}, field: {}", key, field);
                    commands.del(key);
                    commands.hset(key, field, tuple);
                    break;
                case "LPUSH":
                    LOG.debug("LPUSH to Redis - key: {}", key);
                    commands.lpush(key, tuple);
                    break;
                case "RPUSH":
                    LOG.debug("RPUSH to Redis - key: {}", key);
                    commands.rpush(key, tuple);
                    break;
                case "SADD":
                    LOG.debug("SADD to Redis - key: {}", key);
                    commands.sadd(key, tuple);
                    break;
                default:
                    LOG.warn("Unsupported Redis command: {}", command);
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error executing Redis command: {}", e.getMessage(), e);
        }
    }

    private void executeCommand(RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> commands, String key, String field, byte[] value) {
        try {
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            Tuple2<String, byte[]> tuple = new Tuple2<>("", value);
            switch (command.toUpperCase()) {
                case "SET":
                    LOG.debug("Writing to Redis cluster - key: {}", key);
                    commands.set(key, tuple);
                    break;
                case "DEL":
                    LOG.debug("Deleting from Redis cluster - key: {}", key);
                    commands.del(key);
                    break;
                case "HSET":
                    if (field == null) {
                        LOG.error("Field is required for HSET command");
                        return;
                    }
                    LOG.debug("HSET to Redis cluster - key: {}, field: {}", key, field);
                    commands.hset(key, field, tuple);
                    break;
                case "DEL_HSET":
                    if (field == null) {
                        LOG.error("Field is required for DEL_HSET command");
                        return;
                    }
                    LOG.debug("DEL then HSET to Redis cluster - key: {}, field: {}", key, field);
                    commands.del(key);
                    commands.hset(key, field, tuple);
                    break;
                case "LPUSH":
                    LOG.debug("LPUSH to Redis cluster - key: {}", key);
                    commands.lpush(key, tuple);
                    break;
                case "RPUSH":
                    LOG.debug("RPUSH to Redis cluster - key: {}", key);
                    commands.rpush(key, tuple);
                    break;
                case "SADD":
                    LOG.debug("SADD to Redis cluster - key: {}", key);
                    commands.sadd(key, tuple);
                    break;
                default:
                    LOG.warn("Unsupported Redis command: {}", command);
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error executing Redis cluster command: {}", e.getMessage(), e);
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
}