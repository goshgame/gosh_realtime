package com.gosh.config;

import com.gosh.util.LogsUtil;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Flink Redis Source 增强版（移除序列化支持）
 * 支持异步操作
 */
public class RedisSource extends RichSourceFunction<Tuple2<String, byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);
    static {
        LogsUtil.setAllLogLevels();
    }

    private final RedisConfig config;
    private final boolean async;

    private transient RedisCommands<String, Tuple2<String, byte[]>> redisCommands;
    private transient RedisAdvancedClusterCommands<String , Tuple2<String, byte[]>> redisClusterCommands;
    private volatile boolean isRunning = true;
    private transient RedisConnectionManager connectionManager;
    private transient boolean isClusterMode;

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
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connectionManager = RedisConnectionManager.getInstance(config);
        this.isClusterMode = config.isClusterMode();
        if (!async) {
            if(this.isClusterMode){
                this.redisClusterCommands = isClusterMode ? connectionManager.getRedisClusterCommands() : null;
            }else{
                this.redisCommands =connectionManager.getRedisCommands();
            }
        }
        LOG.info("Redis Source opened with config: {}, async: {}", config, async);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }

    @Override
    public void run(SourceContext<Tuple2<String, byte[]>> ctx) throws Exception {
        LOG.info("Starting Redis Source");

        while (isRunning) {
            try {
                List<String> keys;
                if (async) {
                    CompletableFuture<List<String>> keysFuture;
                    if (isClusterMode) {
                        keysFuture = connectionManager.executeWithRetry(
                                () -> connectionManager.executeClusterAsync(
                                        commands -> commands.keys(config.getKeyPattern())
                                )  , 3
                        );
                    } else {
                        keysFuture = connectionManager.executeWithRetry(
                                () -> connectionManager.executeAsync(
                                        commands -> commands.keys(config.getKeyPattern())
                                )  , 3
                        );
                    }
                    keys = keysFuture.get();
                } else {
                    // 同步读取键
                    if (isClusterMode) {
                        keys = redisClusterCommands.keys(config.getKeyPattern());
                    } else {
                        keys = redisCommands.keys(config.getKeyPattern());
                    }
                }
                processKeys(ctx, keys);
                Thread.sleep(5000); // 间隔查询
            } catch (Exception e) {
                LOG.error("Error reading from Redis: {}", e.getMessage(), e);
                Thread.sleep(10000);
            }
        }
    }

    /**
     * 处理键列表（直接返回字节数据）
     */
    private void processKeys(SourceContext<Tuple2<String, byte[]>> ctx, List<String> keys) {
        for (String key : keys) {
            Tuple2<String, byte[]> data = null;
            try{
                switch (config.getValueType().toLowerCase()) {
                    case "string":
                    case "bytes":
                        data = async ?
                                connectionManager.executeStringAsync(commands -> commands.get(key)).join() :
                                connectionManager.getStringCommands().get(key);
                        break;
                    case "list":
                        List<Tuple2<String, byte[]>> listValues = async ?
                                connectionManager.executeListAsync(commands -> commands.lrange(key, 0, 0)).join() :
                                connectionManager.getListCommands().lrange(key, 0, 0);
                        if (!listValues.isEmpty()) {
                            data = listValues.get(0);
                        }
                        break;
                    case "set":
                        data = async ?
                                connectionManager.executeSetAsync(commands -> commands.srandmember(key)).join() :
                                connectionManager.getSetCommands().srandmember(key);
                        break;
                    case "hash":
                        List<Tuple2<String, byte[]>> hashValues = async ?
                                connectionManager.executeHashAsync(commands -> commands.hvals(key)).join() :
                                connectionManager.getHashCommands().hvals(key);
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
        connectionManager.shutdown();
        LOG.info("Redis Source closed");
    }
}