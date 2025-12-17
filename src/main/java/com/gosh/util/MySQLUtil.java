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
 * 重构为支持多实例，以适应多作业场景
 */
public class MySQLUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLUtil.class);
    private static final String MYSQL_CONFIG_PREFIX = "mysql.";
    private static final Pattern CONFIG_PATTERN = Pattern.compile("mysql\\.([^.]+)\\.(.+)");
    
    // 实例变量，每个实例独立管理配置和线程池
    private final Map<String, MySQLConfig> mysqlConfigs;
    private final Map<String, ExecutorService> executorServices;
    
    // 静态单例，保持向后兼容性
    private static volatile MySQLUtil instance;
    private static final Object INSTANCE_LOCK = new Object();



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
     * 私有构造函数，创建新的MySQLUtil实例
     */
    private MySQLUtil() {
        this.mysqlConfigs = new HashMap<>();
        this.executorServices = new HashMap<>();
        loadMySQLConfigs();
        // 不再初始化所有数据源的线程池，改为按需创建
    }
    
    /**
     * 获取静态单例实例（保持向后兼容性）
     */
    private static MySQLUtil getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new MySQLUtil();
                }
            }
        }
        return instance;
    }
    
    /**
     * 创建新的MySQLUtil实例（多作业场景使用）
     */
    public static MySQLUtil createNewInstance() {
        return new MySQLUtil();
    }
    
    /**
     * 静态初始化方法（保持向后兼容性）
     */
    private static void initialize() {
        getInstance();
    }

    /**
     * 从配置文件加载所有MySQL数据源配置（实例方法）
     */
    private void loadMySQLConfigs() {
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
     * 初始化所有数据源的线程池（实例方法，已废弃）
     * 建议使用按需创建线程池的方式，避免资源浪费
     */
    @Deprecated
    private void initExecutors() {
        for (String dsName : mysqlConfigs.keySet()) {
            createExecutorForDataSource(dsName);
        }
    }

    /**
     * 获取数据库连接（静态方法，保持向后兼容性）
     */
    public static Connection getConnection(String dsName) throws SQLException, ClassNotFoundException {
        return getInstance().getConnectionInternal(dsName);
    }
    
    /**
     * 获取数据库连接（静态方法，支持动态数据库名称）
     * @param dsName 数据源名称
     * @param dbName 数据库名称（可选，如果提供则替换URL中的数据库名）
     */
    public static Connection getConnection(String dsName, String dbName) throws SQLException, ClassNotFoundException {
        return getInstance().getConnectionInternal(dsName, dbName);
    }
    
    /**
     * 获取数据库连接（实例方法）
     */
    public Connection getConnectionInternal(String dsName) throws SQLException, ClassNotFoundException {
        MySQLConfig config = getConfigInternal(dsName);
        Class.forName(config.getDriverClass());
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }
    
    /**
     * 获取数据库连接（实例方法，支持动态数据库名称）
     * @param dsName 数据源名称
     * @param dbName 数据库名称（可选，如果提供则替换URL中的数据库名）
     */
    public Connection getConnectionInternal(String dsName, String dbName) throws SQLException, ClassNotFoundException {
        MySQLConfig config = getConfigInternal(dsName);
        Class.forName(config.getDriverClass());
        
        String url = config.getUrl();
        if (dbName != null && !dbName.isEmpty()) {
            // 替换URL中的数据库名称
            url = replaceDatabaseInUrl(url, dbName);
        }
        
        return DriverManager.getConnection(url, config.getUsername(), config.getPassword());
    }
    
    /**
     * 替换JDBC URL中的数据库名称
     * @param originalUrl 原始URL
     * @param newDbName 新数据库名称
     * @return 替换后的URL
     */
    public static String replaceDatabaseInUrl(String originalUrl, String newDbName) {
        // 正则表达式：匹配URL中的数据库名称部分
        // 格式：jdbc:mysql://host:port/dbName?params
        String regex = "(jdbc:mysql://[^:/]+(:[0-9]+)?/)[^/?]+(\\?.*)?";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(originalUrl);
        
        if (matcher.matches()) {
            String prefix = matcher.group(1);
            String params = matcher.group(3) != null ? matcher.group(3) : "";
            return prefix + newDbName + params;
        }
        
        // 如果URL格式不符合预期，返回原始URL
        LOG.warn("无法替换URL中的数据库名称，URL格式不符合预期: {}", originalUrl);
        return originalUrl;
    }

    /**
     * 异步执行数据库操作（静态方法，保持向后兼容性）
     * @param dsName 数据源名称
     * @param task 数据库任务
     * @return 异步结果
     */
    public static <T> CompletableFuture<T> executeAsync(String dsName, Supplier<T> task) {
        return getInstance().executeAsyncInternal(dsName, task);
    }
    
    /**
     * 异步执行数据库操作（实例方法）
     * @param dsName 数据源名称
     * @param task 数据库任务
     * @return 异步结果
     */
    public <T> CompletableFuture<T> executeAsyncInternal(String dsName, Supplier<T> task) {
        // 按需创建线程池
        ExecutorService executor = executorServices.computeIfAbsent(dsName, this::createExecutorForDataSource);
        return CompletableFuture.supplyAsync(task, executor)
                .exceptionally(ex -> {
                    LOG.error("异步任务执行失败", ex);
                    throw new CompletionException(ex);
                });
    }
    
    /**
     * 为指定数据源创建线程池
     */
    private ExecutorService createExecutorForDataSource(String dsName) {
        MySQLConfig config = getConfigInternal(dsName);
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
                        Thread thread = new Thread(r, "mysql-exec-" + dsName + "-" + count++);
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        LOG.info("为数据源[{}]创建线程池: core={}, max={}, queue={}",
                dsName,
                config.getCorePoolSize(),
                config.getMaxThreadPoolSize(),
                config.getQueueCapacity());
        return executor;
    }
    
    /**
     * 为指定数据源初始化线程池
     */
    public static void initExecutorForDataSource(String dsName) {
        MySQLUtil instance = getInstance();
        // 检查数据源是否存在
        instance.getConfigInternal(dsName);
        // 按需创建线程池
        instance.executorServices.computeIfAbsent(dsName, instance::createExecutorForDataSource);
    }

    /**
     * 获取所有MySQL数据源配置（静态方法，保持向后兼容性）
     */
    public static Map<String, MySQLConfig> getAllConfigs() {
        return getInstance().getAllConfigsInternal();
    }
    
    /**
     * 获取所有MySQL数据源配置（实例方法）
     */
    public Map<String, MySQLConfig> getAllConfigsInternal() {
        return Collections.unmodifiableMap(mysqlConfigs);
    }

    /**
     * 根据名称获取指定数据源配置（静态方法，保持向后兼容性）
     */
    public static MySQLConfig getConfig(String dsName) {
        return getInstance().getConfigInternal(dsName);
    }
    
    /**
     * 根据名称获取指定数据源配置（实例方法）
     */
    public MySQLConfig getConfigInternal(String dsName) {
        MySQLConfig config = mysqlConfigs.get(dsName);
        if (config == null) {
            throw new IllegalArgumentException("未找到数据源配置: " + dsName);
        }
        return config;
    }

    /**
     * 获取默认数据源配置（静态方法，保持向后兼容性）
     */
    public static MySQLConfig getDefaultConfig() {
        return getInstance().getDefaultConfigInternal();
    }
    
    /**
     * 获取默认数据源配置（实例方法）
     */
    public MySQLConfig getDefaultConfigInternal() {
        if (mysqlConfigs.isEmpty()) {
            throw new RuntimeException("未配置任何MySQL数据源");
        }
        return mysqlConfigs.values().iterator().next();
    }

    /**
     * 关闭所有线程池（静态方法，关闭单例实例的资源）
     */
    public static void shutdown() {
        if (instance != null) {
            instance.shutdownInternal();
            synchronized (INSTANCE_LOCK) {
                instance = null;
            }
        }
    }
    
    /**
     * 关闭当前实例的所有线程池（实例方法）
     */
    public void shutdownInternal() {
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
            executorServices.clear();
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