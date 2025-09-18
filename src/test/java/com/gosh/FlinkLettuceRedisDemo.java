package com.gosh;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.gosh.entity.RecUserFeatureOuterClass.RecUserFeature;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.*;

public class FlinkLettuceRedisDemo {

    // 加载Redis配置
    private static Properties loadRedisConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream is = FlinkLettuceRedisDemo.class.getClassLoader()
                .getResourceAsStream("redis-config.properties")) {
            props.load(is);
        }
        return props;
    }

    // 序列化RecUserFeature为字符串
    private static String serializeRecUserFeature(RecUserFeature feature) {
        return String.format("uid: \"%s\"\n" +
                        "user_foru_explive_cnt_24h: %d\n" +
                        "user_foru_joinlive_cnt_24h: %d\n" +
                        "user_quitlive_3latest: \"%s\"",
                feature.getKey(),
                feature.getResutls());
    }

    // 自定义异步Redis Sink (Lettuce实现)
    public static class LettuceRedisSink extends RichSinkFunction<RecUserFeature> {
        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connection;
        private RedisAsyncCommands<String, String> asyncCommands;
        private ExecutorService threadPool;
        private Properties redisProps;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            redisProps = loadRedisConfig();

            // 构建Redis连接信息
            RedisURI redisURI = RedisURI.builder()
                    .withHost(redisProps.getProperty("hostname", "localhost"))
                    .withPort(Integer.parseInt(redisProps.getProperty("port", "6379")))
                    .withTimeout(Duration.ofMillis(Long.parseLong(redisProps.getProperty("timeout", "2000"))))
                    .withDatabase(Integer.parseInt(redisProps.getProperty("database", "0")))
                    .withSsl(Boolean.parseBoolean(redisProps.getProperty("ssl", "false")))
                    .build();

            // 如果有密码则添加密码配置
            String password = redisProps.getProperty("password");
            if (password != null && !password.isEmpty()) {
                redisURI.setPassword(password);
            }

            // 初始化Lettuce客户端
            redisClient = RedisClient.create(redisURI);
            connection = redisClient.connect();
            asyncCommands = connection.async();

            // 初始化线程池
            threadPool = new ThreadPoolExecutor(
                    Integer.parseInt(redisProps.getProperty("thread.pool.core.size", "5")),
                    Integer.parseInt(redisProps.getProperty("thread.pool.max.size", "20")),
                    Long.parseLong(redisProps.getProperty("thread.pool.keepalive.time", "60")),
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(Integer.parseInt(redisProps.getProperty("thread.pool.queue.capacity", "1000"))),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }

        @Override
        public void invoke(RecUserFeature value, Context context) {
            String key = "user:feature:" + value.getKey();
            String valueStr = serializeRecUserFeature(value);

            // 提交异步写入任务
            threadPool.submit(() -> {
                try {
                    // 使用Lettuce的异步API执行SET命令
                    RedisFuture<String> future = asyncCommands.set(key, valueStr);
                    // 同步等待结果（也可使用回调处理）
                    String result = future.get();
                    if ("OK".equals(result)) {
                        System.out.println("Lettuce写入成功: " + key);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("写入被中断: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("Lettuce写入失败: " + e.getMessage());
                }
            });
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 关闭线程池
            if (threadPool != null) {
                threadPool.shutdown();
                if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            }
            // 关闭Lettuce连接
            if (connection != null) {
                connection.close();
            }
            // 关闭客户端
            if (redisClient != null) {
                redisClient.shutdown();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成测试数据
        RecUserFeature testFeature = RecUserFeature.newBuilder()
                .setKey("rec:user_feature:12345")
                .setResutls("{'user_foru_explive_cnt_24h':10,'user_foru_joinlive_cnt_24h':3,'user_quitlive_3latest':'live_678:300|live_910:150'}")
                .build();

        // 写入Redis
        env.fromElements(testFeature)

                .addSink(new LettuceRedisSink());

        env.execute("Flink Lettuce Redis Writer Demo");
    }
}