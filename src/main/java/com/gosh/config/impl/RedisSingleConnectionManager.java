package com.gosh.config.impl;

import com.gosh.config.RedisConfig;
import com.gosh.config.RedisConnectionManager;
import com.gosh.serial.StringByteArrayCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class RedisSingleConnectionManager implements RedisConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSingleConnectionManager.class);
    private final RedisConfig config;
    private final RedisClient redisClient;
    private final ExecutorService threadPool;
    private final String connectionKey;

    public RedisSingleConnectionManager(RedisConfig config) {
        this.config = config;
        this.connectionKey = getConnectionKey(config);
        this.redisClient = createClient(config);
        this.threadPool = createThreadPool(config);
    }

    @Override
    public RedisStringCommands<String, byte[]> getStringCommands() {
        try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisListCommands<String, byte[]> getListCommands() {
        StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec());
        return connection.sync();
    }

    @Override
    public RedisSetCommands<String, byte[]> getSetCommands() {
        try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisHashCommands<String, byte[]> getHashCommands() {
        try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeListAsync(Function<RedisListCommands<String, byte[]>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
                RedisListCommands<String, byte[]> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, byte[]>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
                RedisStringCommands<String, byte[]> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String相关操作（如get/set）
            } catch (Exception e) {
                LOG.error("Async String operation failed (single mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, byte[]>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
                RedisSetCommands<String, byte[]> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeHashAsync(Function<RedisHashCommands<String, byte[]>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
                RedisHashCommands<String, byte[]> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public RedisCommands<String, byte[]> getRedisCommands() {
        StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec());
        return connection.sync();
    }

    @Override
    public RedisAdvancedClusterCommands<String, byte[]> getRedisClusterCommands() {
        return null;
    }

    @Override
    public StatefulConnection<String, byte[]> getRedisConnection() {
        return redisClient.connect(new StringByteArrayCodec());
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, byte[]>, T> operation) {
        return executeAsync(operation, null);
    }

    @Override
    public <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, byte[]>, T> operation, String threadPoolName) {
        String poolName = threadPoolName != null ? threadPoolName : connectionKey;
        return CompletableFuture.supplyAsync(() -> {
            try (StatefulRedisConnection<String, byte[]> connection = redisClient.connect(new StringByteArrayCodec())) {
                RedisCommands<String, byte[]> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async Redis operation failed: {}", e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }


    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, byte[]>, T> operation) {
        throw new UnsupportedOperationException("Cluster operations not supported in single node mode");

    }

    @Override
    public <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, byte[]>, T> operation, String threadPoolName) {
        throw new UnsupportedOperationException("Cluster operations not supported in single node mode");
    }

    @Override
    public void shutdown() {
        try {
            redisClient.shutdown();
            threadPool.shutdown();
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
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

        return RedisClient.create(uriBuilder.build());
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

                        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                        scheduler.schedule(() -> {
                            retryOperation(operation, maxRetries, currentAttempt + 1, resultFuture);
                            scheduler.shutdown();
                        }, backoff, TimeUnit.MILLISECONDS);
                    } else {
                        resultFuture.completeExceptionally(ex);
                    }
                    return null;
                });
    }

}