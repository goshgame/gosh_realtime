package com.gosh.util;

import com.gosh.cons.CommonConstants;
import com.gosh.monitor.BackpressureMonitor;
import com.gosh.monitor.DataSkewMonitor;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.WebSubmissionJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Flink 1.20 监控工具类，整合反压监控、作业状态监控及飞书通知功能
 */
public class FlinkMonitorUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMonitorUtil.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    private static final ExecutorService NOTIFY_EXECUTOR = Executors.newSingleThreadExecutor();

    // 实例变量
    private static Configuration flinkConfig;
    private static String jobName;
    private static DataSkewMonitor skewMonitor;
    private static BackpressureMonitor<?> backpressureMonitor;
    private static boolean initialized = false;
    private static FlinkMonitorUtil instance; // 新增：单例实例变量


    static {
        try {
            flinkConfig = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.FLINK_DEFAULT_CONF);
        } catch (Exception e) {
            LOG.warn("加载默认监控配置失败，使用默认值", e);
        }
    }
    
    /**
     * 私有构造函数，通过工厂方法创建实例
     */
    private FlinkMonitorUtil(String jobName, String operatorName) {
        this.jobName = jobName;
        this.flinkConfig = new Configuration(flinkConfig); // 创建配置副本，避免多实例冲突
        
        // 初始化数据倾斜监控
        this.skewMonitor = new DataSkewMonitor(jobName);
        
        // 根据配置决定是否启用反压监控
        boolean enableBackpressureMonitor = flinkConfig.getBoolean("flink.monitor.backpressure.enabled", true);
        if (enableBackpressureMonitor && operatorName != null && !operatorName.isEmpty()) {
            this.backpressureMonitor = new BackpressureMonitor<>(jobName, operatorName);
            LOG.info("反压监控已启用，将监控算子: {}", operatorName);
        } else {
            this.backpressureMonitor = null;
            if (!enableBackpressureMonitor) {
                LOG.info("反压监控已禁用（通过配置）");
            } else {
                LOG.info("反压监控未启用，缺少有效的算子名称");
            }
        }
        
        MessageUtil.setJobName(jobName);
        this.initialized = true;
        LOG.info("Flink 1.20监控初始化完成，作业名称: {}", jobName);
    }
    
    /**
     * 创建FlinkMonitorUtil实例的工厂方法
     */
    public static FlinkMonitorUtil create(String jobName, String operatorName) {
        FlinkMonitorUtil monitor = new FlinkMonitorUtil(jobName, operatorName);
        monitor.startMonitoring();
        instance = monitor; // 初始化单例实例
        return monitor;
    }
    
    /**
     * 启动所有监控任务
     */
    public void startMonitoring() {
        if (initialized) {
            LOG.warn("监控已初始化，避免重复调用initialize方法，当前作业名称: {}", jobName);
            return;
        }
        if (flinkConfig.getBoolean("rest.enable", false)) {
            LOG.info("Flink REST API已启用，监控功能将使用REST API增强监控能力");
        }
        MessageUtil.setJobName(jobName);

        shutdownExistingMonitors();

        skewMonitor = new DataSkewMonitor(jobName);
        skewMonitor.startSkewMonitor();

        if (backpressureMonitor != null) {
            backpressureMonitor.startBackpressureCheck();  // 启动反压检查调度
        }

        initialized = true; // 标记为已初始化
        LOG.info("Flink 1.20监控初始化完成，作业名称: {}", jobName);
    }

    /**
     * 包装Flink执行环境，添加异常监控和反压监控支持
     */
    public static StreamExecutionEnvironment wrapMonitorEnvironment(StreamExecutionEnvironment env) {
        env.getConfig().setGlobalJobParameters(flinkConfig);
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            String errorMsg = String.format("Flink作业[%s]发生未捕获异常: %s", jobName, throwable.getMessage());
            LOG.error(errorMsg, throwable);
            MessageUtil.sendLarkshuMsg(errorMsg, throwable);
        });

        // Checkpoint监控提示
        if (flinkConfig.getBoolean("execution.checkpointing.enabled", false)) {
            LOG.info("将监控Checkpoint状态，作业名称: {}", jobName);
        }
        return env;
    }

    /**
     * 执行Flink作业并监控生命周期
     */
    public static void executeWithMonitor(StreamExecutionEnvironment env, String jobName)
            throws Exception {
        if (!initialized) {
            initialize(jobName, "default-operator");
            if (instance != null) {
                instance.startMonitoring();
            }
        } else {
            LOG.warn("作业[{}]执行时监控已初始化，跳过重复初始化", jobName);
        }
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName(jobName);

        try {
            JobClient jobClient = env.executeAsync(streamGraph);
            LOG.info("作业[{}]已提交，JobID: {}", jobName, jobClient.getJobID());
            MessageUtil.sendLarkshuMsg(String.format("Flink作业[%s]已提交，JobID: %s", jobName, jobClient.getJobID()), null);

            monitorJobStatus(jobClient, jobName);
        } catch (Exception e) {
            String errorMsg = String.format("Flink作业[%s]执行异常: %s", jobName, e.getMessage());
            LOG.error(errorMsg, e);
            MessageUtil.sendLarkshuMsg(errorMsg, e);
            throw e;
        }
    }



    /**
     * 监控作业状态直到完成或失败
     */
    private static void monitorJobStatus(JobClient jobClient, String jobName) throws Exception {
        if (jobClient instanceof WebSubmissionJobClient) {
            LOG.info("检测到Web Submission模式，将使用REST API监控作业状态");
            String restAddress = flinkConfig.getString("rest.address", "localhost");
            int restPort = flinkConfig.getInteger("rest.port", 8081);
            String jobId = jobClient.getJobID().toString();
            String statusUrl = String.format("http://%s:%d/jobs/%s/status", restAddress, restPort, jobId);

            // 使用REST API定时查询状态
            ScheduledExecutorService statusMonitor = Executors.newSingleThreadScheduledExecutor();
            statusMonitor.scheduleAtFixedRate(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(statusUrl))
                            .timeout(Duration.ofSeconds(10))
                            .GET()
                            .build();

                    HttpResponse<String> response = HTTP_CLIENT.send(
                            request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        // 解析JSON响应获取状态（示例：{"status":"RUNNING"}）
                        Map<String, Object> statusJson = OBJECT_MAPPER.readValue(response.body(), Map.class);
                        String status = (String) statusJson.get("status");
                        JobStatus jobStatus = JobStatus.valueOf(status);
                        LOG.info("作业[{}]当前状态: {}", jobName, jobStatus);

                        if (jobStatus.isTerminalState()) {
                            String msg = String.format("Flink作业[%s]已终止，状态: %s", jobName, jobStatus);
                            LOG.info(msg);
                            MessageUtil.sendLarkshuMsg(msg, null);
                            statusMonitor.shutdown();
                        }
                    } else {
                        LOG.warn("查询作业状态失败，响应码: {}", response.statusCode());
                    }
                } catch (Exception e) {
                    LOG.error("REST API监控作业状态失败", e);
                    statusMonitor.shutdown();
                }
            }, 0, 30, TimeUnit.SECONDS);
        } else {
            ScheduledExecutorService statusMonitor = Executors.newSingleThreadScheduledExecutor();
            statusMonitor.scheduleAtFixedRate(() -> {
                try {
                    JobStatus status = jobClient.getJobStatus().get();
                    LOG.info("作业[{}]当前状态: {}", jobName, status);

                    if (status.isTerminalState()) {
                        String msg = String.format("Flink作业[%s]已终止，状态: %s", jobName, status);
                        LOG.info(msg);
                        MessageUtil.sendLarkshuMsg(msg, null);
                        statusMonitor.shutdown();
                    }
                } catch (Exception e) {
                    LOG.error("监控作业状态失败", e);
                    statusMonitor.shutdown();
                }
            }, 0, 30, TimeUnit.SECONDS);
        }
    }

    // 新增：包装KeyedProcessFunction，自动跟踪Key
    public static <K, I, O> KeyedProcessFunction<K, I, O> wrapKeyedProcessFunction(
            KeyedProcessFunction<K, I, O> originalFunction,
            String operatorName,
            Function<K, String> keyToStringConverter) {
        return new KeyedProcessFunction<K, I, O>() {
            @Override
            public void processElement(I value, Context ctx, Collector<O> out) throws Exception {
                K currentKey = ctx.getCurrentKey();
                String keyStr = keyToStringConverter != null
                        ? keyToStringConverter.apply(currentKey)
                        : String.valueOf(currentKey);
                skewMonitor.trackKeyCount(operatorName, keyStr, 1);
                originalFunction.processElement(value, ctx, out);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                originalFunction.open(parameters);
            }

            @Override
            public void close() throws Exception {
                originalFunction.close();
            }
        };
    }

    public static void initialize(String jobName, String operatorName) {
        if (initialized) {
            LOG.warn("监控已初始化，当前作业名称: {}", jobName);
            return;
        }
        MessageUtil.setJobName(jobName);

        shutdownExistingMonitors();

        // 为每个作业创建独立的监控实例
        skewMonitor = new DataSkewMonitor(jobName);
        skewMonitor.startSkewMonitor();

        initialized = true;
        LOG.info("Flink监控初始化完成，作业名称: {}", jobName);
    }

    /**
     * 关闭所有监控资源
     */
    public static void shutdown() {
        // 关闭告警线程池
        NOTIFY_EXECUTOR.shutdown();
        try {
            if (!NOTIFY_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                NOTIFY_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            NOTIFY_EXECUTOR.shutdownNow();
        }


        shutdownExistingMonitors();

        initialized = false;
        LOG.info("Flink监控资源已关闭");
    }

    /**
     * 关闭已存在的监控实例
     */
    private static void shutdownExistingMonitors() {
        if (skewMonitor != null) {
            skewMonitor.shutdown();
            skewMonitor = null;
        }
        if (backpressureMonitor != null) {
            backpressureMonitor.shutdown();
            backpressureMonitor = null;
        }
    }
}