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
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (cluster mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisListCommands<String, Tuple2<String, byte[]>> getListCommands() {
        return connection.sync();
    }

    @Override
    public RedisSetCommands<String, Tuple2<String, byte[]>> getSetCommands() {
        try{
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (cluster mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisHashCommands<String, Tuple2<String, byte[]>> getHashCommands() {
        try{
            return connection.sync(); // 直接返回String命令接口（集群自动路由）
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (cluster mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeListAsync(Function<RedisListCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RedisListCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async cluster list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try{
                RedisStringCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String操作（集群自动分片）
            } catch (Exception e) {
                LOG.error("Async String operation failed (cluster mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try{
                RedisSetCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String操作（集群自动分片）
            } catch (Exception e) {
                LOG.error("Async String operation failed (cluster mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeHashAsync(Function<RedisHashCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try{
                RedisHashCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String操作（集群自动分片）
            } catch (Exception e) {
                LOG.error("Async String operation failed (cluster mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public RedisCommands<String, Tuple2<String, byte[]>> getRedisCommands() {
        return null;
    }

    @Override
    public RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> getRedisClusterCommands() {
        return connection.sync();
    }

    @Override
    public StatefulConnection<String, Tuple2<String, byte[]>> getRedisConnection() {
        return clusterClient.connect(new StringTupleCodec());
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return executeAsync(operation, null);
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        return null;
    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return executeClusterAsync(operation, null);
    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>>, T> operation, String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : connectionKey;
        return CompletableFuture.supplyAsync(() -> {
            try{
                RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async Redis cluster operation failed: {}", e.getMessage(), e);
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
                        LOG.warn("Operation failed, retrying in {}ms (attempt {}/{})", backoff, currentAttempt + 1, maxRetries, ex);

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

}