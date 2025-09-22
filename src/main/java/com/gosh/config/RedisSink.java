package com.gosh.config;


import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
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
public class RedisSink<T, M extends Message> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConfig config;
    private final boolean async;
    private final int batchSize;
    private final Class<M> protoClass; // 存储Protobuf消息类的Class（可序列化）
    private final Function<M, String> keyExtractor; // 从protobuf消息提取key的函数
    private final Function<M, String> fieldExtractor; // 从protobuf消息提取field的函数（用于HSET）

    private transient Parser<M> protoParser; // protobuf解析器
    private transient RedisCommands<String, byte[]> redisCommands;
    private transient RedisAdvancedClusterCommands<String, byte[]> redisClusterCommands;
    private transient AtomicInteger pendingOperations;
    private transient RedisConnectionManager connectionManager;
    private transient boolean isClusterMode;


    // 全参数构造函数（核心）
    public RedisSink(RedisConfig config, boolean async, int batchSize,
                     Class<M> protoClass,
                     Function<M, String> keyExtractor,
                     Function<M, String> fieldExtractor) {
        this.config = config;
        this.async = async;
        this.batchSize = batchSize;
        this.protoClass = protoClass; // 存储Class（可序列化）
        this.keyExtractor = keyExtractor;
        this.fieldExtractor = fieldExtractor;
    }

    // 简化构造函数（默认field提取器）
    public RedisSink(RedisConfig config, boolean async, int batchSize,
                     Class<M> protoClass,
                     Function<M, String> keyExtractor) {
        this(config, async, batchSize, protoClass, keyExtractor, new DefaultFieldExtractor<>());
    }

    // 兼容原有Properties构造（需指定protobuf相关参数）
    public RedisSink(Properties props, Class<M> protoClass, Function<M, String> keyExtractor) {
        this(RedisConfig.fromProperties(props), false, 1, protoClass, keyExtractor);
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
        Method parserMethod = protoClass.getMethod("parser"); // Protobuf生成类都有static的parser()方法
        this.protoParser = (Parser<M>) parserMethod.invoke(null); // 调用静态方法获取Parser

        //System.out.println("config:" + config.toString());
        LOG.info("Redis Sink opened with config: {}, async: {}, batchSize: {}", config, async, batchSize);
    }
    // 添加连接状态检查方法
    private boolean isRunning() {
        try {
            if (isClusterMode) {
                return redisClusterCommands != null &&
                        connectionManager.getRedisClusterCommands() != null;
            } else {
                return redisCommands != null &&
                        connectionManager.getRedisCommands() != null;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
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

        // 2. 区分value类型，打印字节数组具体内容
        if (value instanceof byte[]) {
            byte[] valueBytes = (byte[]) value;
            System.out.println("invoke: value类型=byte[], value内容（字节数组）=" + Arrays.toString(valueBytes));
            // 可选：如果需要查看字符串形式（需确保字节数组是UTF-8编码）
            try {
                String valueStr = new String(valueBytes, "UTF-8");
                System.out.println("invoke: value字符串形式（UTF-8）=" + valueStr);
            } catch (Exception e) {
                System.out.println("invoke: 字节数组转字符串失败（非UTF-8编码）");
            }
        } else {
            System.out.println("invoke: value类型=" + value.getClass().getSimpleName() + ", value内容=" + value);
        }

        if (async) {
            CompletableFuture<Void> future;
            // 根据模式选择对应的异步执行方法
            if (isClusterMode) {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeClusterAsync(commands -> {
                            executeCommand(commands, value);
                            return null;
                        }),
                        3
                );
            } else {
                future = connectionManager.executeWithRetry(
                        () -> connectionManager.executeAsync(commands -> {
                            executeCommand(commands, value);
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
                            byte[] data = (byte[]) value;
                            M message = protoParser.parseFrom(data);
                            String key = keyExtractor.apply(message);
                            System.out.println("设置TTL, key=" + key + ", TTL=" + config.getTtl());
//                            if(isClusterMode){
//                                connectionManager.executeClusterAsync(commands ->{
//                                    commands.expire(key, config.getTtl());
//                                    return  null;
//                                }).get(5, TimeUnit.SECONDS);
//                                System.out.println("集群模式设置TTL完成");
//                            } else{
//                                connectionManager.executeAsync(commands ->{
//                                    commands.expire(key, config.getTtl());
//                                    return  null;
//                                }).get(5, TimeUnit.SECONDS);
//                                System.out.println("单机模式设置TTL完成");
//                            }
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
                executeCommand(redisClusterCommands, value);
            } else{
                executeCommand(redisCommands, value);
            }
        }
    }

    /**
     * 执行Redis命令（使用传入的protobuf解析器）
     */
    private void executeCommand(RedisCommands<String, byte[]> commands, T value) {
        try {
            // 假设输入值为字节数组（与原有逻辑保持一致）
            byte[] data = (byte[]) value;
            // 使用传入的解析器解析protobuf
            M message = protoParser.parseFrom(data);
            LOG.info("传入参数：{}"  , message.toString());
            String key = keyExtractor.apply(message);
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            switch (command.toUpperCase()) {
                case "SET":
                    commands.set(key, data);
                    break;
                case "LPUSH":
                    commands.lpush(config.getKeyPattern(), data);
                    break;
                case "RPUSH":
                    commands.rpush(config.getKeyPattern(), data);
                    break;
                case "SADD":
                    commands.sadd(config.getKeyPattern(), data);
                    break;
                case "HSET":
                    String field = fieldExtractor.apply(message);
                    commands.hset(config.getKeyPattern(), field, data);
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
    private void executeCommand(RedisAdvancedClusterCommands<String, byte[]> commands, T value) {
        try {
            // 假设输入值为字节数组（与原有逻辑保持一致）
            byte[] data = (byte[]) value;
            // 使用传入的解析器解析protobuf
            M message = protoParser.parseFrom(data);
            LOG.info("传入参数：{}"  , message.toString());
            String key = keyExtractor.apply(message);
            String command = config.getCommand();
            if (command == null || command.trim().isEmpty()) {
                LOG.error("Redis command is not configured");
                return;
            }

            switch (command.toUpperCase()) {
                case "SET":
                    commands.set(key, data);
                    break;
                case "LPUSH":
                    commands.lpush(config.getKeyPattern(), data);
                    break;
                case "RPUSH":
                    commands.rpush(config.getKeyPattern(), data);
                    break;
                case "SADD":
                    commands.sadd(config.getKeyPattern(), data);
                    break;
                case "HSET":
                    String field = fieldExtractor.apply(message);
                    commands.hset(config.getKeyPattern(), field, data);
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