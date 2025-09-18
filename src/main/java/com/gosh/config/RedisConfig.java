package com.gosh.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Redis 连接配置类（增强版）
 */
public class RedisConfig implements Serializable {
    private String hostname;
    private int port;
    private String password;
    private int database;
    private String keyPattern;
    private String valueType;
    private String command;
    private int timeout;
    private boolean ssl;
    //private boolean cluster;

    //cluster 支持
    private boolean clusterMode;
    private List<String> clusterNodes; // 集群节点列表，格式: host:port
    private boolean sslEnabled;
    private String sslTrustStore;
    private String sslTrustStorePassword;

    // 序列化配置
    private String serializerType; // protobuf, string, json, bytes
    private String protobufMessageType; // Protobuf 消息类型的全限定类名


    // 线程池配置
    private int threadPoolCoreSize;
    private int threadPoolMaxSize;
    private long threadPoolKeepAliveTime;
    private int threadPoolQueueCapacity;

    public RedisConfig() {
        // 默认配置
        this.hostname = "localhost";
        this.port = 6379;
        this.database = 0;
        this.timeout = 2000;
        this.ssl = true;
        //this.cluster = true;

        // cluster 配置
        this.clusterMode = false;
        this.clusterNodes = Arrays.asList("localhost:6379");
        this.sslEnabled = false;
        this.sslTrustStore = null;
        this.sslTrustStorePassword = null;

        // 默认线程池配置
        this.threadPoolCoreSize = 5;
        this.threadPoolMaxSize = 20;
        this.threadPoolKeepAliveTime = 60L;
        this.threadPoolQueueCapacity = 1000;
    }

    public static RedisConfig fromProperties(Properties props) {
        RedisConfig config = new RedisConfig();

        config.hostname = props.getProperty("hostname", "localhost");
        config.port = Integer.parseInt(props.getProperty("port", "6379"));
        config.password = props.getProperty("password", null);
        config.database = Integer.parseInt(props.getProperty("database", "0"));
        config.keyPattern = props.getProperty("key-pattern", "*");
        config.valueType = props.getProperty("value-type", "string");
        config.command = props.getProperty("command", "SET");
        config.timeout = Integer.parseInt(props.getProperty("timeout", "2000"));
        config.ssl = Boolean.parseBoolean(props.getProperty("ssl", "false"));
        //config.cluster = Boolean.parseBoolean(props.getProperty("cluster", "false"));

        // 线程池配置
        config.threadPoolCoreSize = Integer.parseInt(props.getProperty("thread.pool.core.size", "5"));
        config.threadPoolMaxSize = Integer.parseInt(props.getProperty("thread.pool.max.size", "20"));
        config.threadPoolKeepAliveTime = Long.parseLong(props.getProperty("thread.pool.keepalive.time", "60"));
        config.threadPoolQueueCapacity = Integer.parseInt(props.getProperty("thread.pool.queue.capacity", "1000"));

        // 2. 集群模式配置
        config.clusterMode = Boolean.parseBoolean(props.getProperty("redis.cluster.mode", "false"));
        String nodes = props.getProperty("redis.cluster.nodes", "");
        if (config.clusterMode && !nodes.isEmpty()) {
            // 集群模式下必须解析节点列表，忽略单机的hostname+port
            config.clusterNodes = Arrays.asList(nodes.split(","));
        } else {
            // 非集群模式下，用hostname+port构建单节点列表（便于统一处理）
            config.clusterNodes = Collections.singletonList(config.hostname + ":" + config.port);
        }

        // 3. SSL配置（统一处理）
        config.sslEnabled = Boolean.parseBoolean(props.getProperty("redis.ssl.enabled", "false"));
        config.sslTrustStore = props.getProperty("redis.ssl.trustStore");
        config.sslTrustStorePassword = props.getProperty("redis.ssl.trustStorePassword");

        return config;
    }

    // Getter 和 Setter 方法
    public String getSerializerType() {
        return serializerType;
    }

    public void setSerializerType(String serializerType) {
        this.serializerType = serializerType;
    }

    public String getProtobufMessageType() {
        return protobufMessageType;
    }

    public void setProtobufMessageType(String protobufMessageType) {
        this.protobufMessageType = protobufMessageType;
    }
    public int getThreadPoolCoreSize() {
        return threadPoolCoreSize;
    }

    public void setThreadPoolCoreSize(int threadPoolCoreSize) {
        this.threadPoolCoreSize = threadPoolCoreSize;
    }

    public int getThreadPoolMaxSize() {
        return threadPoolMaxSize;
    }

    public void setThreadPoolMaxSize(int threadPoolMaxSize) {
        this.threadPoolMaxSize = threadPoolMaxSize;
    }

    public long getThreadPoolKeepAliveTime() {
        return threadPoolKeepAliveTime;
    }

    public void setThreadPoolKeepAliveTime(long threadPoolKeepAliveTime) {
        this.threadPoolKeepAliveTime = threadPoolKeepAliveTime;
    }

    public int getThreadPoolQueueCapacity() {
        return threadPoolQueueCapacity;
    }

    public void setThreadPoolQueueCapacity(int threadPoolQueueCapacity) {
        this.threadPoolQueueCapacity = threadPoolQueueCapacity;
    }

//    public boolean isCluster() {
//        return cluster;
//    }
//
//    public void setCluster(boolean cluster) {
//        this.cluster = cluster;
//    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public boolean isClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }

    public List<String> getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(List<String> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public String getSslTrustStore() {
        return sslTrustStore;
    }

    public void setSslTrustStore(String sslTrustStore) {
        this.sslTrustStore = sslTrustStore;
    }

    public String getSslTrustStorePassword() {
        return sslTrustStorePassword;
    }

    public void setSslTrustStorePassword(String sslTrustStorePassword) {
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", password='" + password + '\'' +
                ", database=" + database +
                ", keyPattern='" + keyPattern + '\'' +
                ", valueType='" + valueType + '\'' +
                ", command='" + command + '\'' +
                ", timeout=" + timeout +
                ", ssl=" + ssl +
                //", cluster=" + cluster +
                ", clusterMode=" + clusterMode +
                ", clusterNodes=" + clusterNodes +
                ", sslEnabled=" + sslEnabled +
                ", sslTrustStore='" + sslTrustStore + '\'' +
                ", sslTrustStorePassword='" + sslTrustStorePassword + '\'' +
                ", serializerType='" + serializerType + '\'' +
                ", protobufMessageType='" + protobufMessageType + '\'' +
                ", threadPoolCoreSize=" + threadPoolCoreSize +
                ", threadPoolMaxSize=" + threadPoolMaxSize +
                ", threadPoolKeepAliveTime=" + threadPoolKeepAliveTime +
                ", threadPoolQueueCapacity=" + threadPoolQueueCapacity +
                '}';
    }
}