package com.gosh.config.impl;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.serial.StringTupleCodec;
import com.gosh.util.TimerUtil;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.netty.util.Timer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class RedisSingleConnectionManager implements RedisConnectionManager, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSingleConnectionManager.class);
    private static final ClientResources CLIENT_RESOURCES;
    
    static {
        CLIENT_RESOURCES = DefaultClientResources.builder()
            .ioThreadPoolSize(4)  // 减少IO线程数
            .computationThreadPoolSize(4)  // 减少计算线程数
            .timer(TimerUtil.getSharedTimer())  // 使用共享的Timer
            .build();
    }

    private final RedisConfig config;
    private transient final RedisClient redisClient;
    private transient final ExecutorService threadPool;
    private final String connectionKey;
    private transient final Timer sharedTimer;
    private transient StatefulRedisConnection<String, Tuple2<String, byte[]>> connection;
    private transient int connectionRetryCount = 0; // 连接重试计数器

    public RedisSingleConnectionManager(RedisConfig config) {
        this.config = config;
        this.connectionKey = getConnectionKey(config);
        this.redisClient = createClient(config);
        this.threadPool = createThreadPool(config);
        this.sharedTimer = TimerUtil.getSharedTimer();
        // 初始化连接（只创建一次）
        this.connection = redisClient.connect(new StringTupleCodec());
    }

    @Override
    public RedisStringCommands<String, Tuple2<String, byte[]>> getStringCommands() {
        try{
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisListCommands<String, Tuple2<String, byte[]>> getListCommands() {
        ensureConnectionValid(); // 确保连接有效
        return connection.sync();
    }

    @Override
    public RedisSetCommands<String, Tuple2<String, byte[]>> getSetCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisHashCommands<String, Tuple2<String, byte[]>> getHashCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeListAsync(Function<RedisListCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureConnectionValid(); // 确保连接有效
                RedisListCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisListCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                    return operation.apply(commands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try{
                ensureConnectionValid(); // 确保连接有效
                RedisStringCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String相关操作（如get/set）
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisStringCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                    return operation.apply(stringCommands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async String operation failed (single mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try{
                ensureConnectionValid(); // 确保连接有效
                RedisSetCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisSetCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                    return operation.apply(commands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeHashAsync(Function<RedisHashCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureConnectionValid(); // 确保连接有效
                RedisHashCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisHashCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                    return operation.apply(commands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public RedisCommands<String, Tuple2<String, byte[]>> getRedisCommands() {
        ensureConnectionValid(); // 确保连接有效
        return connection.sync();
    }

    @Override
    public RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> getRedisClusterCommands() {
        return null;
    }

    @Override
    public StatefulConnection<String, Tuple2<String, byte[]>> getRedisConnection() {
        ensureConnectionValid(); // 确保连接有效
        return connection;  // 返回已有的连接实例
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return executeAsync(operation, null);
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : connectionKey;
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureConnectionValid(); // 确保连接有效
                RedisCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                    return operation.apply(commands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async Redis operation failed: {}", e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }


    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation) {
        throw new UnsupportedOperationException("Cluster operations not supported in single node mode");

    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        throw new UnsupportedOperationException("Cluster operations not supported in single node mode");
    }

    @Override
    public void shutdown() {
        try {
            // 先关闭线程池并等待所有任务完成
            threadPool.shutdown();
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                List<Runnable> remaining = threadPool.shutdownNow();
                LOG.warn("尚有 {} 个未完成的任务被强制终止", remaining.size());
            }

            // 关闭redis 连接
            if (connection != null) {
                connection.close();
            }
            redisClient.shutdown();
        } catch (Exception e) {
            LOG.error("Error shutting down single Redis connection manager", e);
        }
    }

    private RedisClient createClient(RedisConfig config) {
        RedisURI.Builder uriBuilder = RedisURI.builder()
                .withHost(config.getHostname())
                .withPort(config.getPort())
                .withDatabase(config.getDatabase())
                .withTimeout(Duration.ofMillis(config.getTimeout()));

        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
            uriBuilder.withPassword(config.getPassword().toCharArray());
        }

        if (config.isSslEnabled()) {
            uriBuilder.withSsl(true);
            configureSsl(config);
        }

        RedisURI uri = uriBuilder.build();
        uri.setTimeout(Duration.ofMillis(config.getTimeout())); // 命令超时

        // 使用共享的 ClientResources 创建 RedisClient
        RedisClient client = RedisClient.create(CLIENT_RESOURCES, uri);
        
        // 配置客户端选项
        ClientOptions clientOptions = ClientOptions.builder()
                .publishOnScheduler(true)  // 使用调度器发布事件
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(config.getTimeout())))
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofMillis(config.getConnectTimeout()))
                        .keepAlive(true)
                        .tcpNoDelay(true)
                        .build())
                .build();
        
        client.setOptions(clientOptions);
        return client;
    }

    private ExecutorService createThreadPool(RedisConfig config) {
        int corePoolSize = config.getThreadPoolCoreSize() > 0 ? config.getThreadPoolCoreSize() : 5;
        int maxPoolSize = config.getThreadPoolMaxSize() > 0 ? config.getThreadPoolMaxSize() : 20;
        long keepAliveTime = config.getThreadPoolKeepAliveTime() > 0 ? config.getThreadPoolKeepAliveTime() : 60L;
        int queueCapacity = config.getThreadPoolQueueCapacity() > 0 ? config.getThreadPoolQueueCapacity() : 1000;

        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("redis-single-pool-" + new AtomicInteger().incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private String getConnectionKey(RedisConfig config) {
        return "single:" + config.getHostname() + ":" + config.getPort() + ":" + config.getDatabase();
    }

    private void configureSsl(RedisConfig config) {
        if (config.getSslTrustStore() != null) {
            System.setProperty("javax.net.ssl.trustStore", config.getSslTrustStore());
            System.setProperty("javax.net.ssl.trustStorePassword", config.getSslTrustStorePassword());
        }
    }

    private void ensureConnectionValid() {
        try {
            // 检查连接是否有效
            if (connection == null || !connection.isOpen()) {
                LOG.info("Redis connection is invalid, reconnecting...");
                reconnect();
            }
            // 发送ping命令检查连接是否可用
            connection.sync().ping();
            // 连接成功，重置重试计数器
            connectionRetryCount = 0;
        } catch (Exception e) {
            LOG.warn("Redis connection is invalid, reconnecting...", e);
            reconnect();
        }
    }

    /**
     * 检查重试次数是否超过限制，如果超过则抛出异常
     * @param cause 异常原因，如果有的话
     */
    private void checkRetryLimitAndThrowIfNeeded(Exception cause) {
        if (connectionRetryCount >= 5) {
            connectionRetryCount = 0;
            String errorMsg = String.format("Failed to reconnect Redis connection (attempt %d/5)", connectionRetryCount);
            if (cause != null) {
                throw new RuntimeException(errorMsg, cause);
            } else {
                throw new RuntimeException(errorMsg);
            }
        }
    }

    private synchronized void reconnect() {
        try {
            // 检查重试次数是否超过限制
            checkRetryLimitAndThrowIfNeeded(null);
            
            // 关闭旧连接
            if (connection != null) {
                connection.close();
            }
            // 重新建立连接
            connection = redisClient.connect(new StringTupleCodec());
            LOG.info("Redis connection reestablished successfully");
            // 连接成功，重置重试计数器
            connectionRetryCount = 0;
        } catch (RuntimeException e) {
            // 重新抛出运行时异常
            throw e;
        } catch (Exception e) {
            // 连接失败，增加重试计数器
            connectionRetryCount++;
            // 当重试次数超过限制时才抛出异常，否则允许继续重试
            checkRetryLimitAndThrowIfNeeded(e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation, int maxRetries) {
        CompletableFuture<T> future = new CompletableFuture<>();

        retryOperation(operation, maxRetries, 0, future);
        return future;
    }

    private <T> void retryOperation(Supplier<CompletableFuture<T>> operation, int maxRetries, int currentAttempt, CompletableFuture<T> resultFuture) {
        operation.get()
                .thenAccept(resultFuture::complete)
                .exceptionally(ex -> {
                    if (currentAttempt < maxRetries) {
                        long backoff = (long) (Math.pow(2, currentAttempt) * 100); // 指数退避
                        LOG.warn(String.format("Operation failed, retrying in %dms (attempt %d/%d)", backoff, currentAttempt + 1, maxRetries), ex);

                        sharedTimer.newTimeout(timeout -> {
                            retryOperation(operation, maxRetries, currentAttempt + 1, resultFuture);
                        }, backoff, TimeUnit.MILLISECONDS);
                    } else {
                        resultFuture.completeExceptionally(ex);
                    }
                    return null;
                });
    }
}