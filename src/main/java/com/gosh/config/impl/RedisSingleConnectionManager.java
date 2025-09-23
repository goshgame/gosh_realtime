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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class RedisSingleConnectionManager implements RedisConnectionManager {
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
    private final RedisClient redisClient;
    private final ExecutorService threadPool;
    private final Timer sharedTimer;
    private final String connectionKey;
    private StatefulRedisConnection<String, Tuple2<String, byte[]>> connection;

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
        //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
        try{
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisListCommands<String, Tuple2<String, byte[]>> getListCommands() {
        //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
        return connection.sync();
    }

    @Override
    public RedisSetCommands<String, Tuple2<String, byte[]>> getSetCommands() {
        //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
        try {
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public RedisHashCommands<String, Tuple2<String, byte[]>> getHashCommands() {
        //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
        try {
            return connection.sync(); // 直接返回String命令接口
        } catch (Exception e) {
            LOG.error("Failed to get RedisStringCommands (single mode)", e);
            throw new RuntimeException("Get String commands failed", e);
        }
    }

    @Override
    public <T> CompletableFuture<T> executeListAsync(Function<RedisListCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
            try {
                RedisListCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
            try{
                RedisStringCommands<String, Tuple2<String, byte[]>> stringCommands = connection.sync();
                return operation.apply(stringCommands); // 执行String相关操作（如get/set）
            } catch (Exception e) {
                LOG.error("Async String operation failed (single mode)", e);
                throw new CompletionException("Async String operation error", e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
            try{
                RedisSetCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public <T> CompletableFuture<T> executeHashAsync(Function<RedisHashCommands<String, Tuple2<String, byte[]>>, T> operation) {
        return CompletableFuture.supplyAsync(() -> {
            //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
            try {
                RedisHashCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
                LOG.error("Async list operation failed", e);
                throw new CompletionException(e);
            }
        }, threadPool);
    }

    @Override
    public RedisCommands<String, Tuple2<String, byte[]>> getRedisCommands() {
        //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
        return connection.sync();
    }

    @Override
    public RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> getRedisClusterCommands() {
        return null;
    }

    @Override
    public StatefulConnection<String, Tuple2<String, byte[]>> getRedisConnection() {
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
            //StatefulRedisConnection<String, String> connection = redisClient.connect(new StringByteArrayCodec());
            try {
                RedisCommands<String, Tuple2<String, byte[]>> commands = connection.sync();
                return operation.apply(commands);
            } catch (Exception e) {
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
}