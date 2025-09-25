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
        LOG.info("参数：{}",configuration);

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 应用配置
        env.configure(configuration, FlinkEnvUtil.class.getClassLoader());

        // 设置状态后端 (Flink 1.20 中推荐使用配置方式，但也可以代码设置)
        try {
            String stateBackend = configuration.getString("state.backend", "hashmap");
            if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                // 对于 RocksDB，可以进一步配置
                RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                        configuration.getString("state.checkpoints.dir", "file:///tmp/flink-checkpoints"));
                env.setStateBackend(rocksDBStateBackend);
            }
        } catch (Exception e) {
            System.err.println("设置状态后端失败: " + e.getMessage());
        }

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