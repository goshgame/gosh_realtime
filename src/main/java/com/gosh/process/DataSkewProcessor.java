package com.gosh.process;

import com.gosh.cons.SkewType;
import com.gosh.monitor.KeyHeatMonitor;
import com.gosh.process.skew.SkewDetector;
import com.gosh.process.skew.SkewHandler;
import com.gosh.process.skew.SkewStrategyFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gosh.util.ConfigurationUtil;
import com.gosh.cons.CommonConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 数据倾斜处理工具类
 * 实现动态Key重分区、并行度调整和负载均衡策略
 */
public class DataSkewProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataSkewProcessor.class);
    private static final Configuration config = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.FLINK_SKEW_CONF);
    private static final int RANDOM_PREFIX_RANGE = config.getInteger("skew.random.prefix.range", 10);
    private static final double SKEW_THRESHOLD = config.getDouble("skew.detection.threshold", 0.7);
    private static final double GLOBAL_SKEW_THRESHOLD = config.getDouble("skew.global.threshold", 50); // MB
    private static final double PARTITION_SKEW_RATIO = config.getDouble("skew.partition.ratio", 2.0);

    private static final long PARTITION_METRIC_WINDOW = config.getLong("skew.partition.metric.window", 10_000); // 指标收集窗口(ms)
    private static final int PARTITION_METRIC_SAMPLES = config.getInteger("skew.partition.metric.samples", 5); // 采样次数

    /**
     * 统一数据倾斜处理入口（支持混合倾斜类型处理）
     */
    public static <T, K> DataStream<T> handleDataSkew(DataStream<T> stream, KeySelector<T, K> keySelector, int baseParallelism) {
        SkewStrategyFactory<T, K> defaultFactory = new DefaultSkewStrategyFactory<>(config);
        return handleDataSkew(stream, keySelector, baseParallelism, defaultFactory);
    }

    /**
     * 通用数据倾斜处理入口（支持用户自定义策略）
     * @param stream 输入数据流
     * @param keySelector Key选择器
     * @param baseParallelism 基础并行度
     * @param strategyFactory 倾斜策略工厂（用户可传入自定义实现）
     */
    public static <T, K> DataStream<T> handleDataSkew(
            DataStream<T> stream,
            KeySelector<T, K> keySelector,
            int baseParallelism,
            SkewStrategyFactory<T, K> strategyFactory) {

        DataStream<T> currentStream = stream;
        SkewType lastSkewType;
        int retryCount = 0;
        int maxRetries = 3;

        SkewDetector<T, K> detector = strategyFactory.getDetector();
        SkewHandler<T> handler = strategyFactory.getHandler();

        do {
            lastSkewType = detector.detect(currentStream, keySelector);
            LOG.info("第{}次检测到数据倾斜类型: {}", retryCount + 1, lastSkewType);

            currentStream = handler.handle(currentStream, lastSkewType, keySelector, baseParallelism);
            retryCount++;

        } while (lastSkewType != SkewType.NONE && retryCount < maxRetries);
        return currentStream;
    }

    /**
     * 增强倾斜类型判断逻辑（支持多类型优先级判断）
     */
    public static <T, K> SkewType detectSkewType(DataStream<T> stream, KeySelector<T, K> keySelector, Configuration config) {
        // 1. 检测全局倾斜（最高优先级）
        long currentDataSize = getCurrentDataSize(stream);
        if (currentDataSize > GLOBAL_SKEW_THRESHOLD * 1024 * 1024) {
            LOG.debug("全局倾斜检测: 数据量={}MB > 阈值={}MB",
                    currentDataSize / 1024 / 1024, GLOBAL_SKEW_THRESHOLD);
            return SkewType.GLOBAL_SKEW;
        }

        // 2. 检测热点Key倾斜（中优先级）
        AtomicReference<Boolean> hasHotKey = new AtomicReference<>(false);
        stream.map(new MapFunction<T, T>() {
            @Override
            public T map(T value) throws Exception {
                K key = keySelector.getKey(value);
                if (isHotKey(key)) {
                    hasHotKey.set(true);
                }
                return value;
            }
        }).setParallelism(1).name("hotkey-skew-detection").disableChaining();

        if (hasHotKey.get()) {
            LOG.debug("热点Key倾斜检测: 发现热点Key");
            return SkewType.HOT_KEY;
        }

        // 3. 检测分区倾斜（低优先级）
        if (detectPartitionSkew(stream)) {
            LOG.debug("分区倾斜检测: 发现分区数据分布不均");
            return SkewType.PARTITION_SKEW;
        }

        return SkewType.NONE;
    }

    /**
     * 优化数据量获取（基于Flink Metrics）
     */
    private static <T> long getCurrentDataSize(DataStream<T> stream) {
        Object globalJobParams = stream.getExecutionConfig().getGlobalJobParameters();
        if (globalJobParams instanceof Configuration) {
            Configuration config = (Configuration) globalJobParams;
            return config.getLong("metrics.input.bytes", 0);
        }
        // 降级为随机值
        return 1024 * 1024 * new Random().nextInt(50);
    }

    /**
     * 处理分区倾斜 - 重分区
     */
    public static <T> DataStream<T> rebalancePartitions(DataStream<T> stream) {
        LOG.info("检测到分区倾斜，执行重分区操作");
        // 使用Flink内置的rebalance策略均匀分布数据
        return stream.rebalance();
    }

    /**
     * 为热点Key添加随机前缀进行重分区
     * @param stream 输入数据流
     * @param keySelector Key选择器
     * @param <T> 数据类型
     * @param <K> Key类型
     * @return 重分区后的数据流
     */
    public static <T, K> DataStream<T> rebalanceHotKey(DataStream<T> stream, KeySelector<T, K> keySelector) {
        LOG.info("启用热点Key重分区策略，随机前缀范围: {}", RANDOM_PREFIX_RANGE);
        
        return stream
                .keyBy(new KeySelector<T, String>() {
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public String getKey(T value) throws Exception {
                        K originalKey = keySelector.getKey(value);
                        // 对热点Key添加随机前缀
                        if (isHotKey(originalKey)) {
                            return random.nextInt(RANDOM_PREFIX_RANGE) + "_" + originalKey.toString();
                        }
                        return originalKey.toString();
                    }
                })
                .map(new MapFunction<T, T>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public T map(T value) throws Exception {
                        // 此处可添加局部预聚合逻辑
                        return value;
                    }
                });
    }

    /**
     * 动态调整算子并行度
     * @param stream 输入数据流
     * @param baseParallelism 基础并行度
     * @param <T> 数据类型
     * @return 调整后的数据流
     */
    public static <T> DataStream<T> adjustParallelism(DataStream<T> stream, int baseParallelism) {
        long currentDataSize = getCurrentDataSize(stream);
        int adjustedParallelism = calculateParallelism(currentDataSize, baseParallelism);
        
        LOG.info("动态调整并行度: 基础={}, 数据量={}, 调整后={}", 
                baseParallelism, currentDataSize, adjustedParallelism);

        // 通过map算子设置并行度，不改变数据内容
        return stream.map(new MapFunction<T, T>() {
            private static final long serialVersionUID = 1L;
            @Override
            public T map(T value) throws Exception {
                return value;
            }
        }).setParallelism(adjustedParallelism);
    }

    /**
     * 检测分区倾斜（基于Flink Metrics实现）
     */
    private static <T> boolean detectPartitionSkew(DataStream<T> stream) {
        // 用于收集分区数据量的容器
        ConcurrentHashMap<Integer, AtomicLong> partitionMetrics = new ConcurrentHashMap<>();
        AtomicReference<Boolean> isPartitionSkew = new AtomicReference<>(false);
        // 存储各轮采样结果
        List<Boolean> sampleResults = new ArrayList<>(PARTITION_METRIC_SAMPLES);

        // 注册分区数据量指标收集器
        DataStream<T> metricStream = stream.process(new ProcessFunction<T, T>() {
            private transient Counter partitionCounter;
            private int subtaskIndex;

            @Override
            public void open(Configuration parameters) {
                subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                // 注册分区计数器Metric
                partitionCounter = getRuntimeContext().getMetricGroup()
                        .addGroup("dataSkew")
                        .counter("partition_records_" + subtaskIndex);
                // 初始化分区计数容器
                partitionMetrics.put(subtaskIndex, new AtomicLong(0));
            }

            @Override
            public void processElement(T value, Context ctx, Collector<T> out) {
                // 累加分区记录数
                long count = partitionCounter.getCount();
                partitionMetrics.get(subtaskIndex).set(count);
                out.collect(value);
            }
        }).name("partition-skew-metric-collector");

        stream = metricStream;
        // 多轮采样逻辑（使用PARTITION_METRIC_SAMPLES参数）
        for (int sample = 0; sample < PARTITION_METRIC_SAMPLES; sample++) {
            final int currentSample = sample + 1;
            LOG.debug("开始第{}轮分区倾斜采样（共{}轮）", currentSample, PARTITION_METRIC_SAMPLES);

            // 重置本轮采样的指标容器
            partitionMetrics.clear();

            // 启动单轮指标分析线程
            Thread analysisThread = new Thread(() -> {
                try {
                    // 等待指标收集窗口
                    Thread.sleep(PARTITION_METRIC_WINDOW);

                    // 收集当前窗口的分区记录数
                    List<Long> partitionCounts = new ArrayList<>();
                    for (AtomicLong counter : partitionMetrics.values()) {
                        partitionCounts.add(counter.get());
                    }

                    // 过滤无数据分区并判断是否倾斜
                    List<Long> validCounts = partitionCounts.stream()
                            .filter(count -> count > 0)
                            .collect(Collectors.toList());

                    boolean currentSampleSkew = false;
                    if (validCounts.size() >= 2) {
                        long maxCount = Collections.max(validCounts);
                        long minCount = Collections.min(validCounts);
                        double maxMinRatio = (double) maxCount / minCount;
                        double cv = calculateCoefficientOfVariation(validCounts);

                        currentSampleSkew = maxMinRatio > PARTITION_SKEW_RATIO || cv > 0.5;
                        LOG.debug("第{}轮采样结果: 最大/最小比={:.2f}, 变异系数={:.2f}, 是否倾斜={}",
                                currentSample, maxMinRatio, cv, currentSampleSkew);
                    }
                    sampleResults.add(currentSampleSkew);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("分区倾斜采样线程被中断", e);
                    sampleResults.add(false); // 异常时按非倾斜处理
                }
            });

            // 执行本轮采样
            analysisThread.start();
            try {
                analysisThread.join(PARTITION_METRIC_WINDOW + 1000); // 等待采样完成
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // 综合多轮采样结果（超过半数采样倾斜则判定为倾斜）
        long skewSampleCount = sampleResults.stream().filter(Boolean::booleanValue).count();
        boolean finalResult = skewSampleCount > PARTITION_METRIC_SAMPLES / 2;

        if (finalResult) {
            LOG.warn("多轮采样综合判定: 共{}轮采样，{}轮检测到倾斜，判定为分区倾斜",
                    PARTITION_METRIC_SAMPLES, skewSampleCount);
        } else {
            LOG.debug("多轮采样综合判定: 共{}轮采样，{}轮检测到倾斜，判定为无分区倾斜",
                    PARTITION_METRIC_SAMPLES, skewSampleCount);
        }

        return finalResult;
    }

    // 计算变异系数（标准差/平均值）
    private static double calculateCoefficientOfVariation(List<Long> counts) {
        if (counts.size() < 2) return 0;

        double mean = counts.stream().mapToLong(Long::longValue).average().orElse(0);
        if (mean == 0) return 0;

        double variance = counts.stream()
                .mapToDouble(x -> Math.pow(x - mean, 2))
                .average()
                .orElse(0);

        return Math.sqrt(variance) / mean;
    }


    /**
     * 使用自定义负载均衡分区器
     * @param stream 输入数据流
     * @param <T> 数据类型
     * @return 均衡后的数据流
     */
    public static <T> DataStream<T> useLoadBalancedPartitioner(DataStream<T> stream) {
        return stream.partitionCustom(new LoadBalancedPartitioner<>(), new KeySelector<T, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String getKey(T value) throws Exception {
                // 根据实际Key提取逻辑实现
                return extractKey(value);
            }
        });
    }

    // 热点Key检测逻辑
    private static <K> boolean isHotKey(K key) {
        return KeyHeatMonitor.isHotKey(key.toString(), SKEW_THRESHOLD);
    }

    // 并行度计算逻辑
    private static int calculateParallelism(long dataSize, int baseParallelism) {
        // 根据数据量和基础并行度计算调整后的并行度
        if (dataSize == 0) return baseParallelism;
        return (int) Math.min(baseParallelism * Math.sqrt(dataSize / 1024 / 1024), 100); // 上限100
    }

    // 自定义负载均衡分区器
    private static class LoadBalancedPartitioner<T> implements Partitioner<String> {
        private static final long serialVersionUID = 1L;
        private int currentIndex = 0;

        @Override
        public int partition(String key, int numPartitions) {
            // 轮询分区策略，均衡热点Key
            currentIndex = (currentIndex + 1) % numPartitions;
            return currentIndex;
        }
    }

    // Key提取逻辑（需根据实际业务实现）
    private static <T> String extractKey(T value) {
        // 实际实现应根据业务数据结构提取Key
        return value.toString();
    }
}
