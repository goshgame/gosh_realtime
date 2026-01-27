package com.gosh.config.impl;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.serial.StringTupleCodec;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.netty.util.Timer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gosh.util.TimerUtil;

import java.io.Serializable;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RedisClusterConnectionManager implements RedisConnectionManager, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterConnectionManager.class);
    private final RedisConfig config;
    private transient final RedisClusterClient clusterClient;
    private transient final ExecutorService threadPool;
    private final String connectionKey;
    private transient final Timer sharedTimer;
    private transient StatefulRedisClusterConnection<String, Tuple2<String, byte[]>> connection;
    private transient int connectionRetryCount = 0; // 连接重试计数器


    public RedisClusterConnectionManager(RedisConfig config) {
        this.config = config;
        this.connectionKey = getConnectionKey(config);
        this.clusterClient = createClusterClient(config);
        this.threadPool = createThreadPool(config);
        this.sharedTimer = TimerUtil.getSharedTimer();
        this.connection = clusterClient.connect(new StringTupleCodec());
    }

    @Override
    public RedisStringCommands<String, Tuple2<String, byte[]>> getStringCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return connection.sync();
            }
            LOG.error("Failed to get RedisStringCommands (cluster mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisListCommands<String, Tuple2<String, byte[]>> getListCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync();
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return connection.sync();
            }
            LOG.error("Failed to get RedisListCommands (cluster mode)", e);
            throw new RuntimeException("Get List commands failed", e);
        }
    }

    @Override
    public RedisSetCommands<String, Tuple2<String, byte[]>> getSetCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return connection.sync();
            }
            LOG.error("Failed to get RedisSetCommands (cluster mode)", e);
            throw new RuntimeException("Get Set commands failed", e);
        }
    }

    @Override
    public RedisHashCommands<String, Tuple2<String, byte[]>> getHashCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return connection.sync();
            }
            LOG.error("Failed to get RedisHashCommands (cluster mode)", e);
            throw new RuntimeException("Get Hash commands failed", e);
        }
    }

    @Override
    public RedisCommands<String, Tuple2<String, byte[]>> getRedisCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return null;
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return null;
            }
            LOG.error("Failed to get RedisCommands (cluster mode)", e);
            throw new RuntimeException("Get Redis commands failed", e);
        }
    }

    @Override
    public RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> getRedisClusterCommands() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return connection.sync();
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return connection.sync();
            }
            LOG.error("Failed to get RedisClusterCommands (cluster mode)", e);
            throw new RuntimeException("Get Redis cluster commands failed", e);
        }
    }

    @Override
    public StatefulConnection<String, Tuple2<String, byte[]>> getRedisConnection() {
        try {
            ensureConnectionValid(); // 确保连接有效
            return clusterClient.connect(new StringTupleCodec());
        } catch (Exception e) {
            if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                LOG.warn("Redis connection was closed, will retry with new connection", e);
                // 连接已关闭，重新建立连接后重试
                ensureConnectionValid();
                return clusterClient.connect(new StringTupleCodec());
            }
            LOG.error("Failed to get RedisConnection (cluster mode)", e);
            throw new RuntimeException("Get Redis connection failed", e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation) {
        throw new UnsupportedOperationException("Single node operations not supported in cluster mode");
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        throw new UnsupportedOperationException("Single node operations not supported in cluster mode");
    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return executeClusterAsync(operation, null);
    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : connectionKey;
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureConnectionValid(); // 确保连接有效
                RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> clusterCommands = connection.sync();
                return operation.apply(clusterCommands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> clusterCommands = connection.sync();
                    return operation.apply(clusterCommands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async Redis cluster operation failed on nodes {}: {}", config.getClusterNodes(), e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
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
                LOG.error("Async Redis list operation failed on nodes {}: {}", config.getClusterNodes(), e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureConnectionValid(); // 确保连接有效
                RedisStringCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                if (e instanceof ClosedChannelException || (e.getCause() != null && e.getCause() instanceof ClosedChannelException)) {
                    LOG.warn("Redis connection was closed, will retry with new connection", e);
                    // 连接已关闭，重新建立连接后重试
                    ensureConnectionValid();
                    RedisStringCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                    return operation.apply(commands);
                } else if (e instanceof UnsupportedOperationException && e.getMessage() != null && e.getMessage().contains("StatusOutput does not support set(long)")) {
                    // 特殊处理这个错误，防止应用崩溃
                    LOG.error("Unsupported operation: " + e.getMessage() + ", returning null to prevent application crash");
                    return null;
                }
                LOG.error("Async Redis string operation failed on nodes {}: {}", config.getClusterNodes(), e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
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
                LOG.error("Async Redis set operation failed on nodes {}: {}", config.getClusterNodes(), e.getMessage(), e);
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
                LOG.error("Async Redis hash operation failed on nodes {}: {}", config.getClusterNodes(), e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
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
            clusterClient.shutdown();

        } catch (Exception e) {
            LOG.error("Error shutting down single Redis connection manager", e);
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

    private RedisClusterClient createClusterClient(RedisConfig config) {
        List<RedisURI> uris = config.getClusterNodes().stream()
                .map(node -> {
                    String[] parts = node.split(":");
                    String host = parts[0];
                    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 6379;

                    RedisURI.Builder uriBuilder = RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withTimeout(Duration.ofMillis(config.getTimeout()))

                            ;

                    if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                        uriBuilder.withPassword(config.getPassword().toCharArray());
                    }
                    if (config.isSslEnabled()) {
                        uriBuilder.withSsl(true);
                        configureSsl(config);
                    }
                    RedisURI uri = uriBuilder.build();
                    //超时设置
                    uri.setTimeout(Duration.ofMillis(config.getTimeout()));
                    return uri;
                })
                .collect(Collectors.toList());

        if (uris.isEmpty()) {
            throw new IllegalArgumentException("Redis cluster nodes cannot be empty");
        }

        return RedisClusterClient.create(uris);
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
                    thread.setName("redis-cluster-pool-" + new AtomicInteger().incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private String getConnectionKey(RedisConfig config) {
        return "cluster:" + String.join(",", config.getClusterNodes());
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
            LOG.error(errorMsg);
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
            connection = clusterClient.connect(new StringTupleCodec());
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
            // 连接失败时，延迟5秒后再重试
            try {
                LOG.info("Sleeping for 5 seconds before retry...");
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Sleep interrupted", ie);
            }
        }
    }
}