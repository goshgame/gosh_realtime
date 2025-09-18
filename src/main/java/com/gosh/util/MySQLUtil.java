package com.gosh.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.Supplier;

import com.gosh.cons.CommonConstants;

/**
 * MySQL 多数据源工具类（带线程池和异步处理）
 * 支持加载多个MySQL数据源配置并提供连接池和异步操作能力
 */
public class MySQLUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLUtil.class);
    private static final String MYSQL_CONFIG_PREFIX = "mysql.";
    private static final Pattern CONFIG_PATTERN = Pattern.compile("mysql\\.([^.]+)\\.(.+)");
    private static volatile Map<String, MySQLConfig> mysqlConfigs;
    static volatile Map<String, ExecutorService> executorServices;
    private static final int ASYNC_TIMEOUT = 5000; // 异步操作超时时间(ms)
    private static final int ASYNC_CAPACITY = 100; // 异步队列容量



    /**
     * MySQL数据源配置封装类（含线程池配置）
     */
    public static class MySQLConfig {
        private String name;
        private String url;
        private String username;
        private String password;
        private String driverClass = "com.mysql.cj.jdbc.Driver";

        // 连接池配置
        private int maxPoolSize = 10;
        private int minIdle = 2;
        private long connectionTimeout = 30000;

        // 线程池配置
        private int corePoolSize = 5;
        private int maxThreadPoolSize = 10;
        private long keepAliveTime = 60;
        private int queueCapacity = 100;

        // Getter和Setter方法
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDriverClass() { return driverClass; }
        public void setDriverClass(String driverClass) { this.driverClass = driverClass; }
        public int getMaxPoolSize() { return maxPoolSize; }
        public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
        public int getMinIdle() { return minIdle; }
        public void setMinIdle(int minIdle) { this.minIdle = minIdle; }
        public long getConnectionTimeout() { return connectionTimeout; }
        public void setConnectionTimeout(long connectionTimeout) { this.connectionTimeout = connectionTimeout; }
        public int getCorePoolSize() { return corePoolSize; }
        public void setCorePoolSize(int corePoolSize) { this.corePoolSize = corePoolSize; }
        public int getMaxThreadPoolSize() { return maxThreadPoolSize; }
        public void setMaxThreadPoolSize(int maxThreadPoolSize) { this.maxThreadPoolSize = maxThreadPoolSize; }
        public long getKeepAliveTime() { return keepAliveTime; }
        public void setKeepAliveTime(long keepAliveTime) { this.keepAliveTime = keepAliveTime; }
        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }

        @Override
        public String toString() {
            return "MySQLConfig{name='" + name + "', url='" + url + "', threadPoolCore=" + corePoolSize + "}";
        }
    }

    /**
     * 初始化配置和线程池
     */
    static void initialize() {
        if (mysqlConfigs == null) {
            synchronized (MySQLUtil.class) {
                if (mysqlConfigs == null) {
                    loadMySQLConfigs();
                    initExecutors();
                }
            }
        }
    }

    /**
     * 从配置文件加载所有MySQL数据源配置
     */
    static void loadMySQLConfigs() {
        mysqlConfigs = new HashMap<>();
        Properties props = new Properties();

        try (InputStream input = MySQLUtil.class.getClassLoader()
                .getResourceAsStream(CommonConstants.MYSQL_DEFAULT_CONF)) {
            if (input == null) {
                throw new RuntimeException("未找到MySQL配置文件: " + CommonConstants.MYSQL_DEFAULT_CONF);
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("加载MySQL配置文件失败", e);
        }

        // 解析配置（格式：mysql.{数据源名}.{属性}）
        Map<String, Map<String, String>> tempConfigs = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(MYSQL_CONFIG_PREFIX)) {
                Matcher matcher = CONFIG_PATTERN.matcher(key);
                if (matcher.matches()) {
                    String dsName = matcher.group(1);
                    String propKey = matcher.group(2);
                    tempConfigs.computeIfAbsent(dsName, k -> new HashMap<>())
                            .put(propKey, props.getProperty(key));
                }
            }
        }

        // 转换为MySQLConfig对象
        for (Map.Entry<String, Map<String, String>> entry : tempConfigs.entrySet()) {
            String dsName = entry.getKey();
            Map<String, String> propMap = entry.getValue();

            MySQLConfig config = new MySQLConfig();
            config.setName(dsName);
            config.setUrl(propMap.get("url"));
            config.setUsername(propMap.get("username"));
            config.setPassword(propMap.get("password"));

            // 处理可选配置
            if (propMap.containsKey("driver-class")) {
                config.setDriverClass(propMap.get("driver-class"));
            }
            if (propMap.containsKey("max-pool-size")) {
                config.setMaxPoolSize(Integer.parseInt(propMap.get("max-pool-size")));
            }
            if (propMap.containsKey("min-idle")) {
                config.setMinIdle(Integer.parseInt(propMap.get("min-idle")));
            }
            if (propMap.containsKey("connection-timeout")) {
                config.setConnectionTimeout(Long.parseLong(propMap.get("connection-timeout")));
            }

            // 线程池配置
            if (propMap.containsKey("thread.core-pool-size")) {
                config.setCorePoolSize(Integer.parseInt(propMap.get("thread.core-pool-size")));
            }
            if (propMap.containsKey("thread.max-pool-size")) {
                config.setMaxThreadPoolSize(Integer.parseInt(propMap.get("thread.max-pool-size")));
            }
            if (propMap.containsKey("thread.keep-alive-time")) {
                config.setKeepAliveTime(Long.parseLong(propMap.get("thread.keep-alive-time")));
            }
            if (propMap.containsKey("thread.queue-capacity")) {
                config.setQueueCapacity(Integer.parseInt(propMap.get("thread.queue-capacity")));
            }

            // 校验必要参数
            if (config.getUrl() == null || config.getUsername() == null) {
                throw new IllegalArgumentException("数据源[" + dsName + "]缺少必要配置（url/username）");
            }

            mysqlConfigs.put(dsName, config);
        }

        LOG.info("成功加载{}个MySQL数据源配置: {}", mysqlConfigs.size(), mysqlConfigs.keySet());
    }

    /**
     * 初始化线程池
     */
    private static void initExecutors() {
        executorServices = new HashMap<>(mysqlConfigs.size());
        for (MySQLConfig config : mysqlConfigs.values()) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    config.getCorePoolSize(),
                    config.getMaxThreadPoolSize(),
                    config.getKeepAliveTime(),
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(config.getQueueCapacity()),
                    new ThreadFactory() {
                        private int count = 0;
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread thread = new Thread(r, "mysql-exec-" + config.getName() + "-" + count++);
                            thread.setDaemon(true);
                            return thread;
                        }
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executorServices.put(config.getName(), executor);
            LOG.info("为数据源[{}]创建线程池: core={}, max={}, queue={}",
                    config.getName(),
                    config.getCorePoolSize(),
                    config.getMaxThreadPoolSize(),
                    config.getQueueCapacity());
        }
    }

    /**
     * 获取数据库连接
     */
    public static Connection getConnection(String dsName) throws SQLException, ClassNotFoundException {
        initialize();
        MySQLConfig config = getConfig(dsName);
        Class.forName(config.getDriverClass());
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }

    /**
     * 异步执行数据库操作
     * @param dsName 数据源名称
     * @param task 数据库任务
     * @return 异步结果
     */
    public static <T> CompletableFuture<T> executeAsync(String dsName, Supplier<T> task) {
        initialize();
        ExecutorService executor = executorServices.get(dsName);
        if (executor == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("未找到数据源线程池: " + dsName));
        }
        return CompletableFuture.supplyAsync(task, executor)
                .exceptionally(ex -> {
                    LOG.error("异步任务执行失败", ex);
                    throw new CompletionException(ex);
                });
    }

    /**
     * 获取所有MySQL数据源配置
     */
    public static Map<String, MySQLConfig> getAllConfigs() {
        initialize();
        return Collections.unmodifiableMap(mysqlConfigs);
    }

    /**
     * 根据名称获取指定数据源配置
     */
    public static MySQLConfig getConfig(String dsName) {
        initialize();
        MySQLConfig config = mysqlConfigs.get(dsName);
        if (config == null) {
            throw new IllegalArgumentException("未找到数据源配置: " + dsName);
        }
        return config;
    }

    /**
     * 获取默认数据源配置
     */
    public static MySQLConfig getDefaultConfig() {
        initialize();
        if (mysqlConfigs.isEmpty()) {
            throw new RuntimeException("未配置任何MySQL数据源");
        }
        return mysqlConfigs.values().iterator().next();
    }

    /**
     * 关闭所有线程池
     */
    public static void shutdown() {
        if (executorServices != null) {
            for (Map.Entry<String, ExecutorService> entry : executorServices.entrySet()) {
                entry.getValue().shutdown();
                try {
                    if (!entry.getValue().awaitTermination(5, TimeUnit.SECONDS)) {
                        entry.getValue().shutdownNow();
                    }
                } catch (InterruptedException e) {
                    entry.getValue().shutdownNow();
                }
                LOG.info("已关闭数据源[{}]的线程池", entry.getKey());
            }
        }
    }


    public static void main(String[] args) {
        // 测试异步执行
        try {
            CompletableFuture<Integer> future = MySQLUtil.executeAsync("db1", () -> {
                try (Connection conn = MySQLUtil.getConnection("db1");
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM t")) { // 拆分资源声明，便于操作ResultSet

                    // 关键修复：先将游标移动到第一行
                    if (rs.next()) {
                        System.out.println(rs.getString(2));
                        return rs.getInt(1); // 此时游标已指向有效行，可安全获取数据
                    }

                    return 0; // 若查询无结果（理论上COUNT(*)不会无结果，此处为防御性处理）
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            future.thenAccept(count -> LOG.info("查询结果数: {}", count))
                    .exceptionally(ex -> {
                        LOG.error("查询失败", ex);
                        return null;
                    }).get();

        } catch (Exception e) {
            LOG.error("测试失败", e);
        } finally {
            MySQLUtil.shutdown();
        }
    }
}