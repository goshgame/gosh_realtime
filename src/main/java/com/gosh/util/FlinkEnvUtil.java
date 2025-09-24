package com.gosh.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import com.gosh.cons.CommonConstants;

/**
 * Flink 1.20 环境工具类
 * 提供创建 DataStream API 和 Table API 环境的公共方法
 */
public class FlinkEnvUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvUtil.class);


    /**
     * 从配置文件加载 Flink 配置
     *
     * @return Flink Configuration 对象
     */
    public static Configuration loadConfigurationFromProperties() {
        Configuration configuration = new Configuration();

        try (InputStream input = FlinkEnvUtil.class.getClassLoader().getResourceAsStream(CommonConstants.FLINK_DEFAULT_CONF)) {
            if (input == null) {
                throw new RuntimeException("无法找到配置文件: " + CommonConstants.FLINK_DEFAULT_CONF);
            }

            Properties props = new Properties();
            props.load(input);

            // 将 Properties 转换为 Flink Configuration
            for (String key : props.stringPropertyNames()) {
                configuration.setString(key, props.getProperty(key));
            }

        } catch (IOException e) {
            throw new RuntimeException("加载配置文件失败: " + CommonConstants.FLINK_DEFAULT_CONF, e);
        }

        return configuration;
    }


    /**
     * 创建并配置 StreamExecutionEnvironment (DataStream API)
     *
     * @return 配置好的 StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment createStreamExecutionEnvironment() {
        Configuration configuration = loadConfigurationFromProperties();
        LOG.info("Loading Flink configuration: {}", configuration);

        // 创建本地执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        System.out.println("Created local environment with configuration");

        // 设置状态后端
        try {
            String stateBackend = configuration.getString("state.backend", "hashmap");
            if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                String checkpointDir = configuration.getString("state.checkpoints.dir", "file:///tmp/flink-checkpoints");
                System.out.println("Setting up RocksDB state backend with checkpoint dir: " + checkpointDir);
                RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(checkpointDir);
                env.setStateBackend(rocksDBStateBackend);
                System.out.println("RocksDB state backend configured successfully");
            }
        } catch (Exception e) {
            System.err.println("Failed to set state backend: " + e.getMessage());
            e.printStackTrace();
        }

        // 打印环境信息
        System.out.println("Environment configuration:");
        System.out.println("- Parallelism: " + env.getParallelism());
        System.out.println("- Max parallelism: " + env.getMaxParallelism());
        System.out.println("- Buffer timeout: " + env.getBufferTimeout());
        System.out.println("- Checkpoint interval: " + configuration.getString("execution.checkpointing.interval", "not set"));

        return env;
    }

    /**
     * 创建并配置 StreamTableEnvironment (Table API)
     * @return 配置好的 StreamTableEnvironment
     */
    public static StreamTableEnvironment createStreamTableEnvironment() {
        Configuration configuration = loadConfigurationFromProperties();

        // 创建流环境
        StreamExecutionEnvironment streamEnv = createStreamExecutionEnvironment();

        // 创建表环境设置 (Flink 1.20 不再使用 BlinkPlanner)
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build();

        return StreamTableEnvironment.create(streamEnv, settings);
    }

    /**
     * 创建并配置纯 TableEnvironment (批处理 Table API)
     * @return 配置好的 TableEnvironment
     */
    public static TableEnvironment createBatchTableEnvironment() {
        Configuration configuration = loadConfigurationFromProperties();

        // 创建表环境设置
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .withConfiguration(configuration)
                .build();

        return TableEnvironment.create(settings);
    }

    /**
     * 创建远程执行环境 (连接到 Flink 集群)
     *
     * @param configPath 配置文件路径
     * @param host JobManager 主机名
     * @param port JobManager 端口
     * @param jarFiles 要上传的 JAR 文件
     * @return 配置好的 StreamExecutionEnvironment
     */
    public static StreamExecutionEnvironment createRemoteStreamExecutionEnvironment(
            String configPath, String host, int port, String... jarFiles) {
        Configuration configuration = loadConfigurationFromProperties();

        // 创建远程执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                host, port, configuration, jarFiles);

        return env;
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment = FlinkEnvUtil.createStreamExecutionEnvironment();

    }
}