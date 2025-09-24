package com.gosh.config;


import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


/**
 * Flink Redis Sink 增强版（支持通用protobuf解析）
 * 支持异步操作、批量操作和动态protobuf类型
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


    // 构造函数调整：移除protobuf相关参数
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

        // 初始化连接管理器（自动识别单机/集群）
        this.connectionManager = RedisConnectionManager.getInstance(config);
        this.isClusterMode = config.isClusterMode();
        if(this.isClusterMode){
            this.redisClusterCommands = isClusterMode ? connectionManager.getRedisClusterCommands() : null;
        }else {
            this.redisCommands = connectionManager.getRedisCommands();
        }
        this.pendingOperations = new AtomicInteger(0);

        // 2. 反射获取Parser：在TaskManager本地初始化，避免序列化
//        Method parserMethod = protoClass.getMethod("parser"); // Protobuf生成类都有static的parser()方法
//        this.protoParser = (Parser<M>) parserMethod.invoke(null); // 调用静态方法获取Parser

        //System.out.println("config:" + config.toString());
        LOG.info("Redis Sink opened with config: {}, async: {}, batchSize: {}", config, async, batchSize);
    }
    // 添加连接状态检查方法
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

        if (value == null) {
            LOG.warn("invoke: 接收为空值(null)，跳过处理");
            return;
        }

        Tuple2<String, byte[]> tuple = (Tuple2<String, byte[]>) value;
        String key = tuple.f0;
        byte[] valueStr = tuple.f1;
        LOG.info("处理数据 - key: {}, value: {}", key, valueStr);

        if (async) {
            CompletableFuture<Void> future;
            // 根据模式选择对应的异步执行方法
            if (isClusterMode) {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeClusterAsync(commands -> {
                            executeCommand(commands, key,valueStr);
                            return null;
                        }),
                        3
                );
            } else {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeAsync(commands -> {
                            executeCommand(commands, key,valueStr);
                            return null;
                        }),
                        3
                );
            }

            if(future !=null){
                //设置 ttl
                future.whenComplete( (r, t) -> {
                    try {
                        if (config.getTtl() > 0) {
                            CompletableFuture<Void> ttlFuture = isClusterMode ?
                                    connectionManager.executeClusterAsync(commands -> {
                                        commands.expire(key, config.getTtl());
                                        return null;
                                    }) :
                                    connectionManager.executeAsync(commands -> {
                                        commands.expire(key, config.getTtl());
                                        return null;
                                    });

                            // 异步处理TTL设置的结果（可选）
                            ttlFuture.exceptionally(ex -> {
                                LOG.error("设置TTL失败, key=" + key, ex);
                                return null;
                            });
                        }
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
                executeCommand(redisClusterCommands, key, valueStr);
            } else{
                executeCommand(redisCommands, key, valueStr);
            }
        }
    }

    /**
     * 执行Redis命令（使用传入的protobuf解析器）
     */
    private void executeCommand(RedisCommands<String, Tuple2<String, byte[]>> commands, String key, byte[] value) {
        try {
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            Tuple2<String, byte[]> stringStringTuple2 = new Tuple2<>("", value);
            switch (command.toUpperCase()) {
                case "SET":
                    LOG.debug("Writing to Redis - key: {}", key);
                    commands.set(key, stringStringTuple2);
                    break;
                case "LPUSH":
                    LOG.debug("LPUSH to Redis - key: {}", config.getKeyPattern());
                    commands.lpush(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "RPUSH":
                    LOG.debug("RPUSH to Redis - key: {}", config.getKeyPattern());
                    commands.rpush(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "SADD":
                    LOG.debug("SADD to Redis - key: {}", config.getKeyPattern());
                    commands.sadd(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "HSET":
                    LOG.debug("HSET to Redis - key: {}", config.getKeyPattern());
                    commands.hset(config.getKeyPattern(), config.getKeyPattern(), stringStringTuple2);
                    break;
                default:
                    LOG.warn("Unsupported Redis command: {}", config.getCommand());
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error writing to Redis: {}", e.getMessage(), e);
        }
    }

    /**
     * 执行Redis集群模式命令
     */
    private void executeCommand(RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> commands, String key, byte[] value) {
        try {
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            Tuple2<String, byte[]> stringStringTuple2 = new Tuple2<>(key, value);
            switch (command.toUpperCase()) {
                case "SET":
                    LOG.debug("Writing to Redis cluster - key: {}", key);
                    commands.set(key, stringStringTuple2);
                    break;
                case "LPUSH":
                    LOG.debug("LPUSH to Redis cluster - key: {}", config.getKeyPattern());
                    commands.lpush(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "RPUSH":
                    LOG.debug("RPUSH to Redis cluster - key: {}", config.getKeyPattern());
                    commands.rpush(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "SADD":
                    LOG.debug("SADD to Redis cluster - key: {}", config.getKeyPattern());
                    commands.sadd(config.getKeyPattern(), stringStringTuple2);
                    break;
                case "HSET":
                    LOG.debug("HSET to Redis cluster - key: {}", config.getKeyPattern());
                    commands.hset(config.getKeyPattern(), key, stringStringTuple2);
                    break;
                default:
                    LOG.warn("Unsupported Redis command: {}", config.getCommand());
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error writing to Redis Cluster: {}", e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        // 等待所有未完成的操作
        if (pendingOperations.get() > 0) {
            LOG.info("等待 {} 个未完成的操作完成...", pendingOperations.get());
            // 等待最多30秒
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