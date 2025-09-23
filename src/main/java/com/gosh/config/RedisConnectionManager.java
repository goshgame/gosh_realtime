package com.gosh.config;

import com.gosh.config.impl.RedisClusterConnectionManager;
import com.gosh.config.impl.RedisSingleConnectionManager;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.*;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public interface RedisConnectionManager {
    // 明确返回支持所有命令的接口（包含list/hash等操作）
    RedisStringCommands<String, Tuple2<String, String>> getStringCommands();
    RedisListCommands<String, Tuple2<String, String>> getListCommands();
    RedisSetCommands<String, Tuple2<String, String>> getSetCommands();
    RedisHashCommands<String, Tuple2<String, String>> getHashCommands();

    // 异步执行方法使用更具体的命令接口
    <T> CompletableFuture<T> executeListAsync(Function<RedisListCommands<String, Tuple2<String, String>>, T> operation);
    <T> CompletableFuture<T> executeStringAsync(Function<RedisStringCommands<String, Tuple2<String, String>>, T> operation);
    <T> CompletableFuture<T> executeSetAsync(Function<RedisSetCommands<String, Tuple2<String, String>>, T> operation);
    <T> CompletableFuture<T> executeHashAsync(Function<RedisHashCommands<String, Tuple2<String, String>>, T> operation);

    RedisCommands<String, Tuple2<String, String>> getRedisCommands();
    RedisAdvancedClusterCommands<String, Tuple2<String, String>> getRedisClusterCommands();
    StatefulConnection<String, Tuple2<String, String>> getRedisConnection();

    <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, String>>, T> operation);

    <T> CompletableFuture<T> executeAsync(Function<RedisCommands<String, Tuple2<String, String>>, T> operation, String threadPoolName);

    <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, String>>, T> operation);
    <T> CompletableFuture<T> executeClusterAsync(Function<RedisAdvancedClusterCommands<String, Tuple2<String, String>>, T> operation, String threadPoolName);

    <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation, int maxRetries);
    void shutdown();
    static RedisConnectionManager getInstance(RedisConfig config) {
        if (config.isClusterMode()) {
            return new RedisClusterConnectionManager(config);
        } else {
            return new RedisSingleConnectionManager(config);
        }
    }
}