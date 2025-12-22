package com.gosh.config;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Flink Redis Sink 增强版（支持通用protobuf解析）
 * 支持异步操作、批量操作和动态protobuf类型
 * 支持 DEL 和 HSET 操作
 */
public class RedisSink<T> extends RichSinkFunction<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConfig config;
    private final boolean async;
    private final int batchSize;
    private final String command; // 保存command字段的副本，避免序列化问题
    private transient RedisCommands<String, Tuple2<String, byte[]>> redisCommands;
    private transient RedisAdvancedClusterCommands<String, Tuple2<String, byte[]>> redisClusterCommands;
    private transient AtomicInteger pendingOperations;
    private transient RedisConnectionManager connectionManager;
    private boolean isClusterMode;
    private transient Map<String, Map<String, Map<String, byte[]>>> batchBuffer; // command -> key -> field -> value
    private transient Map<String, List<byte[]>> listBatchBuffer; // 用于LPUSH/RPUSH的缓冲区：key -> values
    private transient Map<String, byte[]> stringBatchBuffer; // 用于SET的缓冲区：key -> value
    private transient Map<String, List<byte[]>> setBatchBuffer; // 用于SADD的缓冲区：key -> values
    private transient AtomicInteger batchCount; // 批处理计数器
    private transient ReadWriteLock bufferLock; // 用于保护缓冲区的读写锁
    private int maxPendingOperations;
    private long batchIntervalMs; // 批处理时间间隔（毫秒）
    private transient long lastFlushTime; // 上次flush的时间（毫秒）

    public RedisSink(RedisConfig config, boolean async, int batchSize) {
        this(config, async, batchSize, 10, 50000); // 默认maxPendingOperations=10，batchIntervalMs=60秒
    }

    public RedisSink(RedisConfig config, boolean async, int batchSize, int maxPendingOperations) {
        this(config, async, batchSize, maxPendingOperations, 5000); // 默认batchIntervalMs=60秒
    }

    public RedisSink(RedisConfig config, boolean async, int batchSize, int maxPendingOperations, long batchIntervalMs) {
        this.config = config;
        this.async = async;
        this.batchSize = batchSize;
        this.maxPendingOperations = maxPendingOperations;
        this.batchIntervalMs = batchIntervalMs;
        // 保存command字段的副本，避免序列化问题
        this.command = config.getCommand() != null ? config.getCommand().toUpperCase() : "SET";
    }

    public RedisSink(Properties props) {
        this(RedisConfig.fromProperties(props), true, 100, 10, 50000);
    }

    public void setMaxPendingOperations(int maxPendingOperations) {
        this.maxPendingOperations = maxPendingOperations;
    }

    public void setBatchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.connectionManager = RedisConnectionManager.getInstance(config);
        this.isClusterMode = config.isClusterMode();
        if(this.isClusterMode){
            this.redisClusterCommands = isClusterMode ? connectionManager.getRedisClusterCommands() : null;
        }else {
            this.redisCommands = connectionManager.getRedisCommands();
        }
        this.pendingOperations = new AtomicInteger(0);
        // 初始化批处理缓冲区
        this.batchBuffer = new ConcurrentHashMap<>();   // 用于HSET/DEL_HMSET等哈希命令
        this.listBatchBuffer = new ConcurrentHashMap<>(); // 用于LPUSH/RPUSH等列表命令
        this.stringBatchBuffer = new ConcurrentHashMap<>(); // 用于SET等字符串命令
        this.setBatchBuffer = new ConcurrentHashMap<>(); // 用于SADD等集合命令
        this.batchCount = new AtomicInteger(0);
        this.lastFlushTime = System.currentTimeMillis();
        this.bufferLock = new ReentrantReadWriteLock();

        LOG.info("Redis Sink opened with config: {}, async: {}, batchSize: {}, batchIntervalMs: {}", config, async, batchSize, batchIntervalMs);
    }

    private boolean isRunning() {
        try {
            if (isClusterMode) {
                return redisClusterCommands != null;
            } else {
                return redisCommands != null;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(T value, Context context) throws Exception {
        // 检查连接是否已关闭
        if (connectionManager == null || !isRunning()) {
            LOG.warn("连接已关闭，跳过处理任务");
            return;
        }

        String key = null;
        String field = null;
        byte[] valueBytes = null;
        Map<String, byte[]> valueMap = null;

        if (value instanceof Tuple2) {
            Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
            if (tuple.f0 == null) {
                LOG.error("Key is null in Tuple2 input");
                return;
            }
            key = tuple.f0.toString();
            if (tuple.f1 instanceof byte[]) {
                valueBytes = (byte[]) tuple.f1;
            } else if (tuple.f1 instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, byte[]> map = (Map<String, byte[]>) tuple.f1;
                valueMap = map;
            }
        } else if (value instanceof Tuple3) {
            Tuple3<?, ?, ?> tuple = (Tuple3<?, ?, ?>) value;
            if (tuple.f0 == null) {
                LOG.error("Key is null in Tuple3 input");
                return;
            }
            key = tuple.f0.toString();
            field = tuple.f1 != null ? tuple.f1.toString() : null;
            valueBytes = (byte[]) tuple.f2;
        }

        if (key == null) {
            LOG.error("Key is required");
            return;
        }

        // 使用保存的command副本，避免序列化问题
        String currentCommand = this.command;
        // if (isClusterMode) {
        //     executeClusterCommand(command, key, field, valueBytes, valueMap);
        // } else {
        //     executeSingleCommand(command, key, field, valueBytes, valueMap);
        // }
        // 将数据加入缓冲区
        boolean added;
        bufferLock.readLock().lock();
        try {
            added = isBatchBuffer(currentCommand, key, field, valueBytes, valueMap);
        } finally {
            bufferLock.readLock().unlock();
        }
        
        if (added) {
            batchCount.incrementAndGet();
            // 检查是否满足批量大小条件或时间条件，如果满足则触发刷盘
            long currentTime = System.currentTimeMillis();
            boolean sizeCondition = batchCount.get() >= batchSize;
            boolean timeCondition = batchIntervalMs > 0 && (currentTime - lastFlushTime) >= batchIntervalMs;
            
            if ((sizeCondition || timeCondition) && pendingOperations.get() < maxPendingOperations) {
                flushBatch();
            }
        } else {
            // 不支持批量处理的命令直接执行
            if (isClusterMode) {
                executeClusterCommand(currentCommand, key, field, valueBytes, valueMap);
            } else {
                executeSingleCommand(currentCommand, key, field, valueBytes, valueMap);
            }
        }
    }

    /**
     * 将数据添加到对应类型的缓冲区
     * @return 是否成功加入缓冲区（仅支持批量的命令返回true）
     */
    private boolean isBatchBuffer(String command, String key, String field, byte[] valueBytes, Map<String, byte[]> valueMap) {
        // 检查批处理缓冲区是否已初始化
        if (batchBuffer == null || stringBatchBuffer == null || listBatchBuffer == null || setBatchBuffer == null) {
            LOG.warn("Batch buffers are not initialized, skipping batch processing,{},{},{},{}",
                            batchBuffer,stringBatchBuffer,listBatchBuffer, setBatchBuffer );
            return false;
        }
        LOG.info("Sink Redis command:{},key:{},field:{},valueBytes:{},value:{}", command,key,field,valueBytes,valueMap );
        switch (command) {
            case "HSET":
            case "DEL_HSET":
                if (field == null || valueBytes == null) {
                    LOG.error("Field and value are required for {} batch command", command);
                    return false;
                }
                // 初始化命令级缓冲区
                batchBuffer.computeIfAbsent(command, k -> new ConcurrentHashMap<>());
                // 初始化key级缓冲区
                Map<String, Map<String, byte[]>> keyBuffer = batchBuffer.get(command);
                keyBuffer.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
                // 添加field-value
                keyBuffer.get(key).put(field, valueBytes);
                return true;

            case "DEL_HMSET":
                if (valueMap == null || valueMap.isEmpty()) {
                    LOG.error("Value map is required for DEL_HMSET batch command");
                    return false;
                }
                batchBuffer.computeIfAbsent(command, k -> new ConcurrentHashMap<>());
                Map<String, Map<String, byte[]>> delHmsetBuffer = batchBuffer.get(command);
                delHmsetBuffer.put(key, valueMap); // 直接覆盖（DEL后全量设置）
                return true;

            case "SET":
                if (valueBytes == null) {
                    LOG.error("Value is required for SET batch command");
                    return false;
                }
                stringBatchBuffer.put(key, valueBytes);
                return true;

            case "LPUSH":
            case "RPUSH":
                if (valueBytes == null) {
                    LOG.error("Value is required for {} batch command", command);
                    return false;
                }
                // 使用CopyOnWriteArrayList确保线程安全
                listBatchBuffer.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(valueBytes);
                return true;
                
            case "SADD":
                if (valueBytes == null) {
                    LOG.error("Value is required for SADD batch command");
                    return false;
                }
                // 使用CopyOnWriteArrayList确保线程安全
                setBatchBuffer.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(valueBytes);
                return true;

            default:
                LOG.warn("Unsupported batch command: {}", command);
                return false;
        }
    }

    /**
     * 按时间间隔刷新缓冲区数据到Redis（不考虑原有条件）
     */


    /**
     * 刷新缓冲区数据到Redis
     */
    private void flushBatch() {
        if (batchCount.get() == 0) {
            return;
        }

        LOG.debug("Flushing batch data, total count: {}", batchCount.get());
        try {
            if (async) {
                int currentOps = pendingOperations.incrementAndGet();
                LOG.info("开始异步批处理写入，当前未完成操作数: {}", currentOps);
                // 异步批量写入（带重试）
                connectionManager.executeWithRetry(this::doAsyncBatchWrite, 3)
                        .orTimeout(30, TimeUnit.SECONDS)
                        .whenComplete((result, ex) -> {
                            int remainingOps = pendingOperations.decrementAndGet();
                                LOG.info("异步批处理完成，剩余未完成操作数: {}", remainingOps);
                                if (ex != null) {
                                LOG.error("Async batch write failed, will retry later", ex);
                                // 记录未完成操作的数量以便调试
                                LOG.error("当前未完成操作数: {}", pendingOperations.get());
                                // 失败时不清空缓冲区，以便后续重试
                            } else {
                                clearBatchBuffers(); // 成功后清空缓冲区
                                LOG.info("异步批处理成功完成，清空缓冲区");
                            }
                        });
            } else {
                // 同步批量写入
                doSyncBatchWrite();
                clearBatchBuffers(); // 成功后清空缓冲区
            }
        } catch (Exception e) {
            LOG.error("Failed to flush batch data", e);
            if (async) {
                pendingOperations.decrementAndGet();
            }
            // 异常情况下不清空缓冲区，以便后续重试
        }
    }

    /**
     * 同步执行批量写入
     */
    private void doSyncBatchWrite() {
        // 检查缓冲区是否已初始化
        if (batchBuffer == null || stringBatchBuffer == null || listBatchBuffer == null || setBatchBuffer == null) {
            LOG.error("Batch buffers are not initialized, skipping sync batch write");
            return;
        }
        
        // 创建缓冲区副本，避免在处理过程中缓冲区被修改
        Map<String, Map<String, Map<String, byte[]>>> batchBufferCopy;
        Map<String, byte[]> stringBatchCopy;
        Map<String, List<byte[]>> listBatchCopy;
        Map<String, List<byte[]>> setBatchCopy;
        
        bufferLock.writeLock().lock();
        try {
            // 检查是否所有缓冲区都为空
            boolean isAllBuffersEmpty = batchBuffer.isEmpty() && stringBatchBuffer.isEmpty() && listBatchBuffer.isEmpty() && setBatchBuffer.isEmpty();
            if (isAllBuffersEmpty) {
                LOG.debug("Synced batch: no data to write (all buffers are empty)");
                return;
            }
            
            // 创建缓冲区副本
            batchBufferCopy = new HashMap<>(batchBuffer);
            stringBatchCopy = new HashMap<>(stringBatchBuffer);
            listBatchCopy = new HashMap<>(listBatchBuffer);
            setBatchCopy = new HashMap<>(setBatchBuffer);
        } finally {
            bufferLock.writeLock().unlock();
        }
        
        // 处理哈希类型命令（HSET/DEL_HSET/DEL_HMSET）
        for (Map.Entry<String, Map<String, Map<String, byte[]>>> commandEntry : batchBufferCopy.entrySet()) {
            String command = commandEntry.getKey();
            Map<String, Map<String, byte[]>> keyBuffer = commandEntry.getValue();

            for (Map.Entry<String, Map<String, byte[]>> keyEntry : keyBuffer.entrySet()) {
                String key = keyEntry.getKey();
                Map<String, byte[]> fieldValues = keyEntry.getValue();

                // 跳过空的fieldValues，避免"Map must not be empty"错误
                if (fieldValues.isEmpty()) {
                    continue;
                }

                // 转换为Lettuce需要的Tuple2格式
                Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                fieldValues.forEach((k, v) -> wrappedMap.put(k, new Tuple2<>("", v)));

                if (isClusterMode) {
                    // 处理DEL_HSET和DEL_HMSET的删除逻辑
                    if ( ("DEL_HSET".equals(command) || "DEL_HMSET".equals(command)) && config != null && config.getTtl() <= 0) {
                        redisClusterCommands.del(key);
                    }
                    redisClusterCommands.hmset(key, wrappedMap);
                    if (config != null && config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                } else {
                    // 处理DEL_HSET和DEL_HMSET的删除逻辑
                    if ( ("DEL_HSET".equals(command) || "DEL_HMSET".equals(command)) && config != null && config.getTtl() <= 0) {
                        redisCommands.del(key);
                    }
                    redisCommands.hmset(key, wrappedMap);
                    if (config != null && config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                }
                LOG.debug("Synced batch {}: key={}, fields={}", command, key, fieldValues.size());
            }
        }

        // 处理字符串命令（SET）
        if (!stringBatchCopy.isEmpty()) {
            Map<String, Tuple2<String, byte[]>> wrappedStrings = new HashMap<>();
            stringBatchCopy.forEach((k, v) -> wrappedStrings.put(k, new Tuple2<>("", v)));

            // 再次检查wrappedStrings是否为空（理论上不应该，但为了安全）
            if (!wrappedStrings.isEmpty()) {
                if (isClusterMode) {
                    redisClusterCommands.mset(wrappedStrings);
                    // 批量设置过期时间
                    stringBatchCopy.keySet().forEach(key -> {
                        if (config != null && config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                    });
                } else {
                    redisCommands.mset(wrappedStrings);
                    stringBatchCopy.keySet().forEach(key -> {
                        if (config != null && config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                    });
                }
                LOG.debug("Synced batch SET: keys={}", stringBatchCopy.size());
            }
        }

        // 处理列表命令（LPUSH/RPUSH）
        String currentCommand = this.command;
        if ((currentCommand.equals("LPUSH") || currentCommand.equals("RPUSH")) && !listBatchCopy.isEmpty()) {
            for (Map.Entry<String, List<byte[]>> entry : listBatchCopy.entrySet()) {
                String key = entry.getKey();
                List<byte[]> values = entry.getValue();
                
                // 跳过空的values列表
                if (values.isEmpty()) {
                    continue;
                }
                
                Tuple2<String, byte[]>[] tupleValues = values.stream()
                        .map(v -> new Tuple2<>("", v))
                        .toArray(Tuple2[]::new);

                if (isClusterMode) {
                    if (currentCommand.equals("LPUSH")) {
                        redisClusterCommands.lpush(key, tupleValues);
                    } else {
                        redisClusterCommands.rpush(key, tupleValues);
                    }
                    if (config != null && config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                } else {
                    if (currentCommand.equals("LPUSH")) {
                        redisCommands.lpush(key, tupleValues);
                    } else {
                        redisCommands.rpush(key, tupleValues);
                    }
                    if (config != null && config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                }
                LOG.debug("Synced batch {}: key={}, elements={}", currentCommand, key, values.size());
            }
        }
        
        // 处理集合命令（SADD）
        if (currentCommand.equals("SADD") && !setBatchCopy.isEmpty()) {
            for (Map.Entry<String, List<byte[]>> entry : setBatchCopy.entrySet()) {
                String key = entry.getKey();
                List<byte[]> values = entry.getValue();
                
                // 跳过空的values列表
                if (values.isEmpty()) {
                    continue;
                }
                
                Tuple2<String, byte[]>[] tupleValues = values.stream()
                        .map(v -> new Tuple2<>("", v))
                        .toArray(Tuple2[]::new);

                if (isClusterMode) {
                    redisClusterCommands.sadd(key, tupleValues);
                    if (config != null && config.getTtl() > 0) {
                        redisClusterCommands.expire(key, config.getTtl());
                    }
                } else {
                    redisCommands.sadd(key, tupleValues);
                    if (config != null && config.getTtl() > 0) {
                        redisCommands.expire(key, config.getTtl());
                    }
                }
                LOG.debug("Synced batch SADD: key={}, elements={}", key, values.size());
            }
        }
    }

    /**
     * 异步执行批量写入
     */
    private CompletableFuture<Void> doAsyncBatchWrite() {
        // 检查缓冲区是否已初始化
        if (batchBuffer == null || stringBatchBuffer == null || listBatchBuffer == null || setBatchBuffer == null) {
            LOG.error("Batch buffers are not initialized, skipping async batch write");
            return CompletableFuture.completedFuture(null);
        }
        
        // 创建缓冲区副本，避免在处理过程中缓冲区被修改
        Map<String, Map<String, Map<String, byte[]>>> batchBufferCopy;
        Map<String, byte[]> stringBatchCopy;
        Map<String, List<byte[]>> listBatchCopy;
        Map<String, List<byte[]>> setBatchCopy;
        
        bufferLock.writeLock().lock();
        try {
            // 检查是否所有缓冲区都为空
            boolean isAllBuffersEmpty = batchBuffer.isEmpty() && stringBatchBuffer.isEmpty() && listBatchBuffer.isEmpty() && setBatchBuffer.isEmpty();
            if (isAllBuffersEmpty) {
                LOG.debug("Async batch: no data to write (all buffers are empty)");
                return CompletableFuture.completedFuture(null);
            }
            
            // 创建缓冲区副本
            batchBufferCopy = new HashMap<>(batchBuffer);
            stringBatchCopy = new HashMap<>(stringBatchBuffer);
            listBatchCopy = new HashMap<>(listBatchBuffer);
            setBatchCopy = new HashMap<>(setBatchBuffer);
        } finally {
            bufferLock.writeLock().unlock();
        }
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // 处理哈希类型命令（HSET/DEL_HSET/DEL_HMSET）
        for (Map.Entry<String, Map<String, Map<String, byte[]>>> commandEntry : batchBufferCopy.entrySet()) {
            String command = commandEntry.getKey();
            Map<String, Map<String, byte[]>> keyBuffer = commandEntry.getValue();

            for (Map.Entry<String, Map<String, byte[]>> keyEntry : keyBuffer.entrySet()) {
                String key = keyEntry.getKey();
                Map<String, byte[]> fieldValues = new HashMap<>(keyEntry.getValue()); // 创建fieldValues的副本
                
                // 跳过空的fieldValues，避免"Map must not be empty"错误
                if (fieldValues.isEmpty()) {
                    continue;
                }
                
                Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                fieldValues.forEach((k, v) -> wrappedMap.put(k, new Tuple2<>("", v)));

                // 保存当前变量的副本，避免lambda表达式中的闭包问题
                final String cmd = command;
                final String currentKey = key;
                final Map<String, Tuple2<String, byte[]>> finalWrappedMap = wrappedMap;
                final int finalFieldCount = fieldValues.size();
                
                // 创建异步任务并添加到列表
                CompletableFuture<Void> future;
                if (isClusterMode) {
                    future = connectionManager.executeClusterAsync(clusterCmd -> {
                        // 处理DEL_HSET和DEL_HMSET的删除逻辑
                        if ( ("DEL_HSET".equals(cmd) || "DEL_HMSET".equals(cmd)) && config != null && config.getTtl() <= 0) {
                            clusterCmd.del(currentKey);
                        }
                        clusterCmd.hmset(currentKey, finalWrappedMap);
                        if (config != null && config.getTtl() > 0) {
                            clusterCmd.expire(currentKey, config.getTtl());
                        }
                        LOG.info("Async batch {} to Redis Cluster - key: {}, fields: {}", cmd, currentKey, finalFieldCount);
                        return null;
                    });
                } else {
                    future = connectionManager.executeAsync(cmdObj -> {
                        // 处理DEL_HSET和DEL_HMSET的删除逻辑
                        if ( ("DEL_HSET".equals(cmd) || "DEL_HMSET".equals(cmd)) && config != null && config.getTtl() <= 0) {
                            cmdObj.del(currentKey);
                        }
                        cmdObj.hmset(currentKey, finalWrappedMap);
                        if (config != null && config.getTtl() > 0) {
                            cmdObj.expire(currentKey, config.getTtl());
                        }
                        LOG.info("Async batch {} to Redis - key: {}, fields: {}", cmd, currentKey, finalFieldCount);
                        return null;
                    });
                }
                futures.add(future);
            }
        }

        // 处理字符串命令（SET）
        if (!stringBatchCopy.isEmpty()) {
            Map<String, Tuple2<String, byte[]>> wrappedStrings = new HashMap<>();
            stringBatchCopy.forEach((k, v) -> wrappedStrings.put(k, new Tuple2<>("", v)));

            // 再次检查wrappedStrings是否为空（理论上不应该，但为了安全）
            if (!wrappedStrings.isEmpty()) {
                CompletableFuture<Void> future;
                if (isClusterMode) {
                    future = connectionManager.executeClusterAsync(cmd -> {
                        cmd.mset(wrappedStrings);
                        wrappedStrings.keySet().forEach(key -> {
                            if (config != null && config.getTtl() > 0) {
                                cmd.expire(key, config.getTtl());
                            }
                        });
                        LOG.info("Async batch SET to Redis Cluster - keys: {}", wrappedStrings.size());
                        return null;
                    });
                } else {
                    future = connectionManager.executeAsync(cmd -> {
                        cmd.mset(wrappedStrings);
                        wrappedStrings.keySet().forEach(key -> {
                            if (config != null && config.getTtl() > 0) {
                                cmd.expire(key, config.getTtl());
                            }
                        });
                        LOG.info("Async batch SET to Redis - keys: {}", wrappedStrings.size());
                        return null;
                    });
                }
                futures.add(future);
            }
        }

        // 处理列表命令（LPUSH/RPUSH）
        String currentCommand = this.command;
        if ((currentCommand.equals("LPUSH") || currentCommand.equals("RPUSH")) && !listBatchCopy.isEmpty()) {
            for (Map.Entry<String, List<byte[]>> entry : listBatchCopy.entrySet()) {
                String key = entry.getKey();
                List<byte[]> values = new ArrayList<>(entry.getValue()); // 创建values的副本
                
                // 跳过空的values列表
                if (values.isEmpty()) {
                    continue;
                }
                
                Tuple2<String, byte[]>[] tupleValues = values.stream()
                        .map(v -> new Tuple2<>("", v))
                        .toArray(Tuple2[]::new);

                CompletableFuture<Void> future;
                if (isClusterMode) {
                    future = connectionManager.executeClusterAsync(cmd -> {
                        if (currentCommand.equals("LPUSH")) {
                            cmd.lpush(key, tupleValues);
                        } else {
                            cmd.rpush(key, tupleValues);
                        }
                        if (config != null && config.getTtl() > 0) {
                            cmd.expire(key, config.getTtl());
                        }
                        LOG.info("Async batch {} to Redis Cluster - key: {}, elements: {}", currentCommand, key, values.size());
                        return null;
                    });
                } else {
                    future = connectionManager.executeAsync(cmd -> {
                        if (currentCommand.equals("LPUSH")) {
                            cmd.lpush(key, tupleValues);
                        } else {
                            cmd.rpush(key, tupleValues);
                        }
                        if (config != null && config.getTtl() > 0) {
                            cmd.expire(key, config.getTtl());
                        }
                        LOG.info("Async batch {} to Redis - key: {}, elements: {}", currentCommand, key, values.size());
                        return null;
                    });
                }
                futures.add(future);
            }
        }
        
        // 处理集合命令（SADD）
        if (currentCommand.equals("SADD") && !setBatchCopy.isEmpty()) {
            for (Map.Entry<String, List<byte[]>> entry : setBatchCopy.entrySet()) {
                String key = entry.getKey();
                List<byte[]> values = new ArrayList<>(entry.getValue()); // 创建values的副本
                
                // 跳过空的values列表
                if (values.isEmpty()) {
                    continue;
                }
                
                Tuple2<String, byte[]>[] tupleValues = values.stream()
                        .map(v -> new Tuple2<>("", v))
                        .toArray(Tuple2[]::new);

                CompletableFuture<Void> future;
                if (isClusterMode) {
                    future = connectionManager.executeClusterAsync(cmd -> {
                        cmd.sadd(key, tupleValues);
                        if (config != null && config.getTtl() > 0) {
                            cmd.expire(key, config.getTtl());
                        }
                        LOG.info("Async batch SADD to Redis Cluster - key: {}, elements: {}", key, values.size());
                        return null;
                    });
                } else {
                    future = connectionManager.executeAsync(cmd -> {
                        cmd.sadd(key, tupleValues);
                        if (config != null && config.getTtl() > 0) {
                            cmd.expire(key, config.getTtl());
                        }
                        LOG.info("Async batch SADD to Redis - key: {}, elements: {}", key, values.size());
                        return null;
                    });
                }
                futures.add(future);
            }
        }

        // 使用CompletableFuture.allOf并行执行所有异步操作
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * 清空所有缓冲区并重置计数
     */
    private void clearBatchBuffers() {
        bufferLock.writeLock().lock();
        try {
            if (batchBuffer != null) {
                batchBuffer.clear();
            }
            if (listBatchBuffer != null) {
                listBatchBuffer.clear();
            }
            if (stringBatchBuffer != null) {
                stringBatchBuffer.clear();
            }
            if (setBatchBuffer != null) {
                setBatchBuffer.clear();
            }
            if (batchCount != null) {
                batchCount.set(0);
            }
            // 更新上次flush的时间
            lastFlushTime = System.currentTimeMillis();
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    


    private void executeSingleCommand(String command, String key, String field, byte[] valueBytes, Map<String, byte[]> valueMap) {
        try {
            if (config == null || connectionManager == null) {
                LOG.error("Config or connectionManager is null");
                return;
            }
            if (async) {
                // 异步执行模式 - 使用重试机制
                boolean incremented = false;
                if (pendingOperations != null) {
                    pendingOperations.incrementAndGet();
                    incremented = true;
                }
                connectionManager.executeWithRetry(
                    () -> {
                        // 使用通用的executeAsync方法
                        return connectionManager.executeAsync(cmd -> {
                            switch (command) {
                                case "SET":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for SET command");
                                    }
                                    LOG.debug("Async SET to Redis - key: {}", key);
                                    cmd.set(key, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "HSET":
                                    if (field == null || valueBytes == null) {
                                        throw new IllegalArgumentException("Field and value are required for HSET command");
                                    }
                                    LOG.debug("Async HSET to Redis - key: {}, field: {}", key, field);
                                    cmd.hset(key, field, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "DEL_HSET":
                                    if (field == null || valueBytes == null) {
                                        throw new IllegalArgumentException("Field and value are required for DEL_HSET command");
                                    }
                                    LOG.debug("Async DEL then HSET to Redis - key: {}, field: {}", key, field);
                                    // 优化：如果TTL大于0，可以考虑使用HSET代替DEL+HSET，利用TTL自动过期
                                    if (config != null && config.getTtl() <= 0) {
                                        cmd.del(key);
                                    }
                                    cmd.hset(key, field, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "DEL_HMSET":
                                    if (valueMap == null || valueMap.isEmpty()) {
                                        throw new IllegalArgumentException("Value map is required for DEL_HMSET command");
                                    }
                                    LOG.debug("Async DEL then HMSET to Redis - key: {}, fields: {}", key, valueMap.size());
                                    // 优化：如果TTL大于0，可以考虑使用HMSET代替DEL+HMSET，利用TTL自动过期
                                    if (config != null && config.getTtl() <= 0) {
                                        cmd.del(key);
                                    }
                                    Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                                    for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                                        wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                                    }
                                    cmd.hmset(key, wrappedMap);
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "LPUSH":
                                case "RPUSH":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for " + command + " command");
                                    }
                                    LOG.debug("Async {} to Redis - key: {}", command, key);
                                    Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                                    if (command.equals("LPUSH")) {
                                        cmd.lpush(key, tuple);
                                    } else {
                                        cmd.rpush(key, tuple);
                                    }
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "SADD":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for SADD command");
                                    }
                                    LOG.debug("Async SADD to Redis - key: {}", key);
                                    cmd.sadd(key, new Tuple2<>("", valueBytes));
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported Redis command: " + command);
                            }
                            return null;
                        });
                    },
                    3 // 最多重试3次
                ).whenComplete((result, ex) -> {
                    pendingOperations.decrementAndGet();
                    if (ex != null) {
                        LOG.error("Async {} command failed after retries: {}", command, ex.getMessage(), ex);
                    }
                });
            } else {
                // 同步执行模式
                switch (command) {
                    case "SET":
                        if (valueBytes == null) {
                            LOG.error("Value is required for SET command");
                            return;
                        }
                        LOG.debug("SET to Redis - key: {}", key);
                        redisCommands.set(key, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "HSET":
                        if (field == null || valueBytes == null) {
                            LOG.error("Field and value are required for HSET command");
                            return;
                        }
                        LOG.debug("HSET to Redis - key: {}, field: {}", key, field);
                        redisCommands.hset(key, field, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "DEL_HSET":
                        if (field == null || valueBytes == null) {
                            LOG.error("Field and value are required for DEL_HSET command");
                            return;
                        }
                        LOG.debug("DEL then HSET to Redis - key: {}, field: {}", key, field);
                        redisCommands.del(key);
                        redisCommands.hset(key, field, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "DEL_HMSET":
                        if (valueMap == null || valueMap.isEmpty()) {
                            LOG.error("Value map is required for DEL_HMSET command");
                            return;
                        }
                        LOG.debug("DEL then HMSET to Redis - key: {}, fields: {}", key, valueMap.size());
                        redisCommands.del(key);
                        Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                        for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                            wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                        }
                        redisCommands.hmset(key, wrappedMap);
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "LPUSH":
                    case "RPUSH":
                        if (valueBytes == null) {
                            LOG.error("Value is required for " + command + " command");
                            return;
                        }
                        LOG.debug("{} to Redis - key: {}", command, key);
                        Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                        if (command.equals("LPUSH")) {
                            redisCommands.lpush(key, tuple);
                        } else {
                            redisCommands.rpush(key, tuple);
                        }
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "SADD":
                        if (valueBytes == null) {
                            LOG.error("Value is required for SADD command");
                            return;
                        }
                        LOG.debug("SADD to Redis - key: {}", key);
                        redisCommands.sadd(key, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisCommands.expire(key, config.getTtl());
                        }
                        break;
                    default:
                        LOG.error("Unsupported Redis command: {}", command);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to execute Redis command: " + command, e);
            if (async && pendingOperations != null) {
                pendingOperations.decrementAndGet();
            }
        }
    }

    private void executeClusterCommand(String command, String key, String field, byte[] valueBytes, Map<String, byte[]> valueMap) {
        try {
            if (config == null || connectionManager == null) {
                LOG.error("Config or connectionManager is null");
                return;
            }
            if (async) {
                // 异步执行模式 - 使用重试机制
                boolean incremented = false;
                if (pendingOperations != null) {
                    pendingOperations.incrementAndGet();
                    incremented = true;
                }
                connectionManager.executeWithRetry(
                    () -> {
                        // 统一使用executeClusterAsync方法
                        return connectionManager.executeClusterAsync(cmd -> {
                            switch (command) {
                                case "SET":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for SET command");
                                    }
                                    LOG.debug("Async SET to Redis Cluster - key: {}", key);
                                    cmd.set(key, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "HSET":
                                    if (field == null || valueBytes == null) {
                                        throw new IllegalArgumentException("Field and value are required for HSET command");
                                    }
                                    LOG.debug("Async HSET to Redis Cluster - key: {}, field: {}", key, field);
                                    cmd.hset(key, field, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "DEL_HSET":
                                    if (field == null || valueBytes == null) {
                                        throw new IllegalArgumentException("Field and value are required for DEL_HSET command");
                                    }
                                    LOG.debug("Async DEL then HSET to Redis Cluster - key: {}, field: {}", key, field);
                                    // 优化：如果TTL大于0，可以考虑使用HSET代替DEL+HSET
                                    if (config != null && config.getTtl() <= 0) {
                                        cmd.del(key);
                                    }
                                    cmd.hset(key, field, new Tuple2<>("", valueBytes));
                                    if (config != null && config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "DEL_HMSET":
                                    if (valueMap == null || valueMap.isEmpty()) {
                                        throw new IllegalArgumentException("Value map is required for DEL_HMSET command");
                                    }
                                    LOG.debug("Async DEL then HMSET to Redis Cluster - key: {}, fields: {}", key, valueMap.size());
                                    // 优化：如果TTL大于0，可以考虑使用HMSET代替DEL+HMSET
                                    if (config.getTtl() <= 0) {
                                        cmd.del(key);
                                    }
                                    Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                                    for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                                        wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                                    }
                                    cmd.hmset(key, wrappedMap);
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "LPUSH":
                                case "RPUSH":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for " + command + " command");
                                    }
                                    LOG.debug("Async {} to Redis Cluster - key: {}", command, key);
                                    Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                                    if (command.equals("LPUSH")) {
                                        cmd.lpush(key, tuple);
                                    } else {
                                        cmd.rpush(key, tuple);
                                    }
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                case "SADD":
                                    if (valueBytes == null) {
                                        throw new IllegalArgumentException("Value is required for SADD command");
                                    }
                                    LOG.debug("Async SADD to Redis Cluster - key: {}", key);
                                    cmd.sadd(key, new Tuple2<>("", valueBytes));
                                    if (config.getTtl() > 0) {
                                        cmd.expire(key, config.getTtl());
                                    }
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported Redis command: " + command);
                            }
                            return null;
                        });
                    },
                    3 // 最多重试3次
                ).whenComplete((result, ex) -> {
                    pendingOperations.decrementAndGet();
                    if (ex != null) {
                        LOG.error("Async {} command failed after retries: {}", command, ex.getMessage(), ex);
                    }
                });
            } else {
                // 同步执行模式
                switch (command) {
                    case "SET":
                        if (valueBytes == null) {
                            LOG.error("Value is required for SET command");
                            return;
                        }
                        LOG.debug("SET to Redis Cluster - key: {}", key);
                        redisClusterCommands.set(key, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "HSET":
                        if (field == null || valueBytes == null) {
                            LOG.error("Field and value are required for HSET command");
                            return;
                        }
                        LOG.debug("HSET to Redis Cluster - key: {}, field: {}", key, field);
                        redisClusterCommands.hset(key, field, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "DEL_HSET":
                        if (field == null || valueBytes == null) {
                            LOG.error("Field and value are required for DEL_HSET command");
                            return;
                        }
                        LOG.debug("DEL then HSET to Redis Cluster - key: {}, field: {}", key, field);
                        redisClusterCommands.del(key);
                        redisClusterCommands.hset(key, field, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "DEL_HMSET":
                        if (valueMap == null || valueMap.isEmpty()) {
                            LOG.error("Value map is required for DEL_HMSET command");
                            return;
                        }
                        LOG.debug("DEL then HMSET to Redis Cluster - key: {}, fields: {}", key, valueMap.size());
                        redisClusterCommands.del(key);
                        Map<String, Tuple2<String, byte[]>> wrappedMap = new HashMap<>();
                        for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
                            wrappedMap.put(entry.getKey(), new Tuple2<>("", entry.getValue()));
                        }
                        redisClusterCommands.hmset(key, wrappedMap);
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "LPUSH":
                    case "RPUSH":
                        if (valueBytes == null) {
                            LOG.error("Value is required for " + command + " command");
                            return;
                        }
                        LOG.debug("{} to Redis Cluster - key: {}", command, key);
                        Tuple2<String, byte[]> tuple = new Tuple2<>("", valueBytes);
                        if (command.equals("LPUSH")) {
                            redisClusterCommands.lpush(key, tuple);
                        } else {
                            redisClusterCommands.rpush(key, tuple);
                        }
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    case "SADD":
                        if (valueBytes == null) {
                            LOG.error("Value is required for SADD command");
                            return;
                        }
                        LOG.debug("SADD to Redis Cluster - key: {}", key);
                        redisClusterCommands.sadd(key, new Tuple2<>("", valueBytes));
                        if (config.getTtl() > 0) {
                            redisClusterCommands.expire(key, config.getTtl());
                        }
                        break;
                    default:
                        LOG.error("Unsupported Redis command: {}", command);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to execute Redis Cluster command: " + command, e);
            if (async && pendingOperations != null) {
                pendingOperations.decrementAndGet();
            }
        }
    }

    @Override
    public void close() throws Exception {
        // 关闭前刷新批处理缓冲区中剩余的数据
        if (batchCount != null && batchCount.get() > 0) {
            LOG.info("关闭前刷新剩余的 {} 条批处理数据", batchCount.get());
            // 同步刷新剩余数据
            doSyncBatchWrite();
            clearBatchBuffers();
        }
        
        // 等待所有异步操作完成
        if (pendingOperations != null && pendingOperations.get() > 0) {
            LOG.info("等待 {} 个未完成的操作完成...", pendingOperations.get());
            long timeout = 60000; // 增加超时时间到60秒
            long interval = 100;
            long waited = 0;
            while (pendingOperations.get() > 0 && waited < timeout) {
                LOG.info("等待异步操作完成，当前未完成数: {}, 已等待: {}ms, 超时阈值: {}ms",
                        pendingOperations.get(), waited, timeout);
                Thread.sleep(interval);
                waited += interval;
            }
            if (pendingOperations.get() > 0) {
                LOG.warn("仍有 {} 个操作未完成，强制关闭", pendingOperations.get());
            }
        }
        
        super.close();
        
        // 关闭连接管理器
        if (connectionManager != null) {
            connectionManager.shutdown();
        }
        
        LOG.info("Redis Sink closed");
    }
}