package com.gosh.config;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Flink Redis Source 增强版（移除序列化支持）
 * 支持异步操作
 */
public class RedisSource extends RichSourceFunction<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);

    private final RedisConfig config;
    private final boolean async;

    private transient RedisCommands<String, byte[]> redisCommands;
    private volatile boolean isRunning = true;

    // 构造函数
    public RedisSource(Properties props) {
        this(RedisConfig.fromProperties(props), false);
    }

    public RedisSource(RedisConfig config) {
        this(config, false);
    }

    public RedisSource(RedisConfig config, boolean async) {
        this.config = config;
        this.async = async;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        if (!async) {
            this.redisCommands = RedisConnectionManager.getRedisCommands(config);
        }
        LOG.info("Redis Source opened with config: {}, async: {}", config, async);
    }

    @Override
    public void run(SourceFunction.SourceContext<byte[]> ctx) throws Exception {
        LOG.info("Starting Redis Source");

        while (isRunning) {
            try {
                List<String> keys;
                if (async) {
                    // 异步读取键
                    CompletableFuture<List<String>> keysFuture = RedisConnectionManager.executeAsync(
                            config,
                            //commands -> commands.keys(config.getKeyPattern())
                            commands ->{
                                // 集群模式下使用集群命令扫描所有节点
                                if (config.isClusterMode()) {
                                    return ((RedisAdvancedClusterCommands<String, byte[]>) commands)
                                            .keys(config.getKeyPattern());
                                } else {
                                    return commands.keys(config.getKeyPattern());
                                }
                            }
                    );
                    keys = keysFuture.get();
                } else {
                    // 同步读取键
                    //keys = redisCommands.keys(config.getKeyPattern());
                    if (config.isClusterMode()) {
                        keys = ((RedisAdvancedClusterCommands<String, byte[]>) redisCommands)
                                .keys(config.getKeyPattern());
                    } else {
                        keys = redisCommands.keys(config.getKeyPattern());
                    }
                }
                processKeys(ctx, keys);
                // 间隔查询
                Thread.sleep(5000);
            } catch (Exception e) {
                LOG.error("Error reading from Redis: {}", e.getMessage(), e);
                Thread.sleep(10000);
            }
        }
    }

    /**
     * 处理键列表（直接返回字节数据）
     */
    private void processKeys(SourceFunction.SourceContext<byte[]> ctx, List<String> keys) {
        for (String key : keys) {
            byte[] data = null;
            try{
                // 根据值类型获取原始字节数据
                switch (config.getValueType().toLowerCase()) {
                    case "string":
                    case "bytes":
                        data = async ?
                                RedisConnectionManager.executeAsync(config, commands -> commands.get(key)).join() :
                                redisCommands.get(key);
                        break;
                    case "list":
                        List<byte[]> listValues = async ?
                                RedisConnectionManager.executeAsync(config, commands -> commands.lrange(key, 0, 0)).join() :
                                redisCommands.lrange(key, 0, 0);
                        if (!listValues.isEmpty()) {
                            data = listValues.get(0);
                        }
                        break;
                    case "set":
                        data = async ?
                                RedisConnectionManager.executeAsync(config, commands -> commands.srandmember(key)).join() :
                                redisCommands.srandmember(key);
                        break;
                    case "hash":
                        List<byte[]> hashValues = async ?
                                RedisConnectionManager.executeAsync(config, commands -> commands.hvals(key)).join() :
                                redisCommands.hvals(key);
                        if (!hashValues.isEmpty()) {
                            data = hashValues.get(0);
                        }
                        break;
                    default:
                        LOG.warn("Unsupported value type: {}", config.getValueType());
                        break;
                }

                if (data != null) {
                    ctx.collect(data); // 直接输出字节数组
                }
            }catch (Exception e) {
                LOG.error("Error processing key: {}", key, e);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        LOG.info("Redis Source cancelled");
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("Redis Source closed");
    }
}