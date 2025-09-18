package com.gosh.config;
import com.gosh.serial.ProtobufSerializer;
import com.gosh.serial.StringByteArrayCodec;
import com.gosh.serial.impl.GenericProtobufSerializer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Redis 连接管理器增强版
 * 使用 Lettuce 客户端管理 Redis 连接，并提供线程池支持
 */
public class RedisConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(RedisConnectionManager.class);
    private static final ConcurrentHashMap<String, Object> CONNECTIONS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ExecutorService> THREAD_POOLS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ProtobufSerializer<?>> SERIALIZERS = new ConcurrentHashMap<>();
    private static final Map<RedisConfig, RedisClusterClient> clusterClients = new ConcurrentHashMap<>();


    // 默认线程池配置
    private static final int DEFAULT_CORE_POOL_SIZE = 5;
    private static final int DEFAULT_MAX_POOL_SIZE = 20;
    private static final long DEFAULT_KEEP_ALIVE_TIME = 60L;
    private static final int DEFAULT_QUEUE_CAPACITY = 1000;

    // 修改方法名和返回类型，返回连接对象（实现 AutoCloseable）
    public static RedisCommands<String, byte[]> getRedisCommands(RedisConfig config) {
        String connectionKey = getConnectionKey(config);
        StringByteArrayCodec codec = new StringByteArrayCodec(); // 使用新编码

        if (config.isClusterMode()) {
            RedisClusterClient clusterClient = (RedisClusterClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClusterClient(config));
            StatefulRedisClusterConnection<String, byte[]> connection = clusterClient.connect(codec);
            return (RedisCommands<String, byte[]>) connection.sync();
        } else {
            RedisClient redisClient = (RedisClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClient(config));
            StatefulRedisConnection<String, byte[]> connection = redisClient.connect(codec);
            return connection.sync();
        }
    }

    /**
     * 获取 Redis 同步命令接口
     */
    public static StatefulConnection<String, byte[]> getRedisConnection(RedisConfig config) {
        String connectionKey = getConnectionKey(config);
        StringByteArrayCodec codec = new StringByteArrayCodec();

        if (config.isClusterMode()) {
            RedisClusterClient clusterClient = (RedisClusterClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClusterClient(config));
            return clusterClient.connect(codec);
        } else {
            RedisClient redisClient = (RedisClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClient(config));
            return redisClient.connect(codec);
        }
    }

    // 同步修改getRedisAsyncCommands方法（确保异步命令接口类型匹配）
    public static RedisClusterAsyncCommands<String, byte[]> getRedisAsyncCommands(RedisConfig config) {
        String connectionKey = getConnectionKey(config);
        StringByteArrayCodec codec = new StringByteArrayCodec();

        if (config.isClusterMode()) {
            RedisClusterClient clusterClient = (RedisClusterClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClusterClient(config));
            StatefulRedisClusterConnection<String, byte[]> connection = clusterClient.connect(codec);
            return connection.async();
        } else {
            RedisClient redisClient = (RedisClient) CONNECTIONS.computeIfAbsent(
                    connectionKey, k -> createClient(config));
            StatefulRedisConnection<String, byte[]> connection = redisClient.connect(codec);
            // 单机异步命令接口转型为集群异步接口（兼容处理，实际是RedisAsyncCommands）
            return (RedisClusterAsyncCommands<String, byte[]>) connection.async();
        }
    }

    /**
     * 获取 Protobuf 序列化器
     */
    @SuppressWarnings("unchecked")
    public static <T extends com.google.protobuf.Message> ProtobufSerializer<T> getProtobufSerializer(RedisConfig config) {
        if (!"protobuf".equals(config.getSerializerType())) {
            throw new IllegalArgumentException("Serializer type is not protobuf: " + config.getSerializerType());
        }

        if (config.getProtobufMessageType() == null) {
            throw new IllegalArgumentException("Protobuf message type is not specified");
        }

        return (ProtobufSerializer<T>) SERIALIZERS.computeIfAbsent(config.getProtobufMessageType(), k -> {
            try {
                Class<?> messageClass = Class.forName(config.getProtobufMessageType());
                if (!com.google.protobuf.Message.class.isAssignableFrom(messageClass)) {
                    throw new IllegalArgumentException("Class is not a protobuf message: " + config.getProtobufMessageType());
                }

                @SuppressWarnings("unchecked")
                Class<T> protobufClass = (Class<T>) messageClass;
                return new GenericProtobufSerializer<>(protobufClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Protobuf message class not found: " + config.getProtobufMessageType(), e);
            }
        });
    }

    /**
     * 使用线程池异步执行 Redis 操作
     */
    public static <T> CompletableFuture<T> executeAsync(RedisConfig config, Function<RedisCommands<String, byte[]>, T> operation) {
        return executeAsync(config, operation, null);
    }

    /**
     * 使用线程池异步执行 Redis 操作（可指定线程池名称）
     */
    public static <T> CompletableFuture<T> executeAsync(RedisConfig config,
                                                        Function<RedisCommands<String, byte[]>, T> operation,
                                                        String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : getConnectionKey(config);
        ExecutorService executor = getThreadPool(poolName, config);

        return CompletableFuture.supplyAsync(() -> {
            try (StatefulConnection<String, byte[]> connection = getRedisConnection(config)) {
                // 统一声明为 RedisCommands 类型（父接口）
                RedisCommands<String, byte[]> commands;

                if (config.isClusterMode()) {
                    // 集群模式：RedisAdvancedClusterCommands 转型为 RedisCommands（安全，因继承关系）
                    StatefulRedisClusterConnection<String, byte[]> clusterConn = (StatefulRedisClusterConnection<String, byte[]>) connection;
                    commands = (RedisCommands<String, byte[]>) clusterConn.sync(); // 返回 RedisAdvancedClusterCommands，自动向上转型为 RedisCommands
                } else {
                    // 单机模式：直接返回 RedisCommands
                    StatefulRedisConnection<String, byte[]> singleConn = (StatefulRedisConnection<String, byte[]>) connection;
                    commands = singleConn.sync(); // 返回 RedisCommands，直接赋值
                }

                return operation.apply(commands); // 此时 commands 类型统一为 RedisCommands，符合 operation 函数参数要求
            } catch (Exception e) {
                LOG.error("Async Redis operation failed: {}", e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, executor);
    }
    /**
     * 使用线程池批量执行 Redis 操作
     */
    public static <T> CompletableFuture<Void> executeBatchAsync(RedisConfig config,
                                                                Iterable<Function<RedisCommands<String, byte[]>, T>> operations,
                                                                CompletionHandler<T> handler) {
        return executeBatchAsync(config, operations, handler, null);
    }

    /**
     * 使用线程池批量执行 Redis 操作（可指定线程池名称）
     */
    public static <T> CompletableFuture<Void> executeBatchAsync(RedisConfig config,
                                                                Iterable<Function<RedisCommands<String, byte[]>, T>> operations,
                                                                CompletionHandler<T> handler,
                                                                String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : getConnectionKey(config);
        ExecutorService executor = getThreadPool(poolName, config);

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        AtomicInteger pendingOperations = new AtomicInteger(0);
        ConcurrentLinkedQueue<CompletableFuture<T>> futures = new ConcurrentLinkedQueue<>();

        // 提交所有操作
        for (Function<RedisCommands<String, byte[]>, T> operation : operations) {
            pendingOperations.incrementAndGet();

            CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
                try (StatefulConnection<String, byte[]> connection = getRedisConnection(config)) {
                    // 统一声明为 RedisCommands 类型（父接口）
                    RedisCommands<String, byte[]> commands;

                    if (config.isClusterMode()) {
                        // 集群模式：RedisAdvancedClusterCommands 转型为 RedisCommands（安全，因继承关系）
                        StatefulRedisClusterConnection<String, byte[]> clusterConn = (StatefulRedisClusterConnection<String, byte[]>) connection;
                        commands = (RedisCommands<String, byte[]>) clusterConn.sync(); // 返回 RedisAdvancedClusterCommands，自动向上转型为 RedisCommands
                    } else {
                        // 单机模式：直接返回 RedisCommands
                        StatefulRedisConnection<String, byte[]> singleConn = (StatefulRedisConnection<String, byte[]>) connection;
                        commands = singleConn.sync(); // 返回 RedisCommands，直接赋值
                    }

                    return operation.apply(commands); // 此时 commands 类型统一为 RedisCommands，符合 operation 函数参数要求
                } catch (Exception e) {
                    LOG.error("Async Redis operation failed: {}", e.getMessage(), e);
                    throw new CompletionException(e);
                }
            }, executor);

            futures.add(future);

            future.whenComplete((result, ex) -> {
                try {
                    if (ex != null) {
                        handler.onError(ex);
                    } else {
                        handler.onSuccess(result);
                    }
                } finally {
                    if (pendingOperations.decrementAndGet() == 0) {
                        resultFuture.complete(null);
                    }
                }
            });
        }

        // 如果没有操作，直接完成
        if (pendingOperations.get() == 0) {
            resultFuture.complete(null);
        }

        return resultFuture;
    }

    /**
     * 获取线程池
     */
    private static ExecutorService getThreadPool(String poolName, RedisConfig config) {
        return THREAD_POOLS.computeIfAbsent(poolName, k -> createThreadPool(config));
    }

    /**
     * 创建线程池
     */
    private static ExecutorService createThreadPool(RedisConfig config) {
        int corePoolSize = config.getThreadPoolCoreSize() > 0 ?
                config.getThreadPoolCoreSize() : DEFAULT_CORE_POOL_SIZE;

        int maxPoolSize = config.getThreadPoolMaxSize() > 0 ?
                config.getThreadPoolMaxSize() : DEFAULT_MAX_POOL_SIZE;

        long keepAliveTime = config.getThreadPoolKeepAliveTime() > 0 ?
                config.getThreadPoolKeepAliveTime() : DEFAULT_KEEP_ALIVE_TIME;

        int queueCapacity = config.getThreadPoolQueueCapacity() > 0 ?
                config.getThreadPoolQueueCapacity() : DEFAULT_QUEUE_CAPACITY;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                new ThreadFactory() {
                    private final AtomicInteger threadCount = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("redis-pool-" + threadCount.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // 当队列满时，由调用线程执行
        );

        LOG.info("Created Redis thread pool: core={}, max={}, keepAlive={}s, queue={}",
                corePoolSize, maxPoolSize, keepAliveTime, queueCapacity);

        return executor;
    }

    /**
     * 创建 Redis 客户端（单机模式）
     */
    private static RedisClient createClient(RedisConfig config) {
        RedisURI.Builder uriBuilder = RedisURI.builder()
                .withHost(config.getHostname())
                .withPort(config.getPort())
                .withDatabase(config.getDatabase())
                .withTimeout(Duration.ofMillis(config.getTimeout()));

        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
            uriBuilder.withPassword(config.getPassword().toCharArray());
        }

        if (config.isSsl()) {
            uriBuilder.withSsl(true);
        }

        RedisURI redisURI = uriBuilder.build();
        return RedisClient.create(redisURI);
    }

    /**
     * 创建 Redis 集群客户端
     */
//    private static RedisClusterClient createClusterClient(RedisConfig config) {
//        RedisURI.Builder uriBuilder = RedisURI.builder()
//                .withHost(config.getHostname())
//                .withPort(config.getPort())
//                .withSsl(config.isSsl())
//                .withTimeout(Duration.ofMillis(config.getTimeout()));
//
//        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
//            uriBuilder.withPassword(config.getPassword().toCharArray());
//        }
//
//        if (config.isSsl()) {
//            uriBuilder.withSsl(true);
//        }
//
//        RedisURI redisURI = uriBuilder.build();
//        return RedisClusterClient.create(redisURI);
//    }
    // 修改创建集群客户端的方法
    private static RedisClusterClient createClusterClient(RedisConfig config) {
        // 从集群节点列表解析RedisURI
        List<RedisURI> uris = config.getClusterNodes().stream()
                .map(node -> {
                    String[] parts = node.split(":");
                    String host = parts[0];
                    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 6379;

                    RedisURI.Builder uriBuilder = RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withTimeout(Duration.ofMillis(config.getTimeout()));

                    if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                        uriBuilder.withPassword(config.getPassword().toCharArray());
                    }
                    if (config.isSslEnabled()) {
                        uriBuilder.withSsl(true);
                        // 可选：配置SSL信任库
                        if (config.getSslTrustStore() != null) {
                            System.setProperty("javax.net.ssl.trustStore", config.getSslTrustStore());
                            System.setProperty("javax.net.ssl.trustStorePassword", config.getSslTrustStorePassword());
                        }
                    }
                    return uriBuilder.build();
                })
                .collect(Collectors.toList());

        if (uris.isEmpty()) {
            throw new IllegalArgumentException("Redis cluster nodes cannot be empty when cluster mode is enabled");
        }

        return RedisClusterClient.create(uris);
    }

    /**
     * 生成连接键
     */
    private static String getConnectionKey(RedisConfig config) {
        return config.getHostname() + ":" + config.getPort() + ":" + config.getDatabase();
    }

    /**
     * 关闭所有连接和线程池
     */
    public static void shutdown() {
        // 关闭所有 Redis 连接
        CONNECTIONS.forEach((key, client) -> {
            try {
                if (client instanceof RedisClient) {
                    ((RedisClient) client).shutdown();
                } else if (client instanceof RedisClusterClient) {
                    ((RedisClusterClient) client).shutdown();
                }
            } catch (Exception e) {
                LOG.error("Error shutting down Redis client: " + key, e);
            }
        });
        CONNECTIONS.clear();

        // 关闭所有线程池
        THREAD_POOLS.forEach((key, executor) -> {
            try {
                executor.shutdown();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        THREAD_POOLS.clear();

        // 清空序列化器缓存
        SERIALIZERS.clear();

        LOG.info("Redis connection manager shutdown completed");
    }

    /**
     * 获取线程池状态信息
     */
    public static String getThreadPoolStatus(String threadPoolName) {
        ExecutorService executor = THREAD_POOLS.get(threadPoolName);
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
            return String.format("Pool: %s, Active: %d, PoolSize: %d, Queue: %d/%d",
                    threadPoolName,
                    tpe.getActiveCount(),
                    tpe.getPoolSize(),
                    tpe.getQueue().size(),
                    tpe.getQueue().remainingCapacity());
        }
        return "Thread pool not found: " + threadPoolName;
    }

    /**
     * 获取所有线程池状态信息
     */
    public static String getAllThreadPoolStatus() {
        StringBuilder sb = new StringBuilder();
        THREAD_POOLS.forEach((name, executor) -> {
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
                sb.append(String.format("Pool: %s, Active: %d, PoolSize: %d, Queue: %d/%d\n",
                        name,
                        tpe.getActiveCount(),
                        tpe.getPoolSize(),
                        tpe.getQueue().size(),
                        tpe.getQueue().remainingCapacity()));
            }
        });
        return sb.toString();
    }

    /**
     * 完成处理器接口
     */
    public interface CompletionHandler<T> {
        void onSuccess(T result);
        void onError(Throwable throwable);
    }
}