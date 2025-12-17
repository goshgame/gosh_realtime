package com.gosh.util;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.source.JdbcSource;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import java.sql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Flink 1.20 MySQL 连接器工具类
 * 提供创建和配置MySQL Flink连接器的静态方法
 */
public class MySQLFlinkUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLFlinkUtil.class);
    private static final int DEFAULT_ASYNC_TIMEOUT = 5000; // 异步操作默认超时时间(ms)
    private static final int DEFAULT_ASYNC_CAPACITY = 100; // 异步队列默认容量
    
    /**
     * 通用数据库值类型转换方法
     * 处理MySQL所有数据类型到Java类型的转换
     */
    private static Object convertDatabaseValue(Object value) {
        if (value == null) {
            return null;
        }
        
        // ==================== 二进制类型处理 ====================
        // 优先处理byte[]类型，避免被其他类型检查干扰
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            // 确保转换为包装类型Byte，而不是原始类型byte
            if (bytes.length > 0) {
                // 将byte[]转换为Byte对象
                return Byte.valueOf(bytes[0]);
            } else {
                return null;
            }
        }
        
        // ==================== 数值类型处理 ====================
        // 整数类型：TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT
        if (value instanceof Number) {
            return value;
        }
        
        // ==================== 日期时间类型处理 ====================
        // DATE类型
        if (value instanceof java.sql.Date) {
            return new java.util.Date(((java.sql.Date) value).getTime());
        }
        
        // DATETIME和TIMESTAMP类型
        if (value instanceof java.sql.Timestamp) {
            return new java.util.Date(((java.sql.Timestamp) value).getTime());
        }
        
        // TIME类型
        if (value instanceof java.sql.Time) {
            return new java.util.Date(((java.sql.Time) value).getTime());
        }
        
        // ==================== 字符串类型处理 ====================
        // CHAR, VARCHAR, TEXT等字符串类型
        if (value instanceof String) {
            return value;
        }
        
        // ==================== 精确小数类型处理 ====================
        // DECIMAL类型
        if (value instanceof java.math.BigDecimal) {
            // 保持BigDecimal类型以确保精度
            return value;
        }
        
        // ==================== JSON类型处理 ====================
        // JSON类型
        if (value instanceof com.mysql.cj.xdevapi.JsonValue) {
            return value.toString();
        }
        
        // ==================== 其他类型处理 ====================
        // BIT类型 (对于BIT(n)，n>1会返回byte[])
        // ENUM类型 (通常返回String或Integer)
        // SET类型 (通常返回String或Integer)
        // GEOMETRY类型 (通常返回byte[]或Geometry对象)
        
        // 对于未明确处理的类型，保持原样返回
        return value;
    }

    /**
     * 创建Flink MySQL Source (批量读取) - 返回Row类型
     * 适用于全量或分页读取MySQL表数据
     */
    public static DataStream<Row> createJdbcSource(
            StreamExecutionEnvironment env,
            String dsName,
            String query,
            int fetchSize) {

        MySQLUtil.MySQLConfig config = MySQLUtil.getConfig(dsName);

        // 构建JdbcSource，指定Row类型并添加默认的Row提取器
        JdbcSource<Row> jdbcSource = JdbcSource.<Row>builder()
                .setDBUrl(config.getUrl())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSql(query)
                .setResultSetFetchSize(fetchSize)
                .setResultExtractor(resultSet -> {
                    // 使用默认的Row提取逻辑，从元数据获取列数
                    try {
                        int columnCount = resultSet.getMetaData().getColumnCount();
                        Row row = new Row(columnCount);
                        for (int i = 0; i < columnCount; i++) {
                            Object value = resultSet.getObject(i + 1);
                            // 通用类型转换处理
                            value = convertDatabaseValue(value);
                            row.setField(i, value);
                        }
                        return row;
                    } catch (SQLException e) {
                        throw new RuntimeException("提取结果集失败", e);
                    }
                })
                .build();
        return env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
    }

    /**
     * 创建Flink MySQL Sink (批量写入)
     */
    public static <T> SinkFunction<T> createJdbcSink(
            String dsName,
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            int batchSize,
            long batchIntervalMs) {

        MySQLUtil.MySQLConfig config = MySQLUtil.getConfig(dsName);

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(batchIntervalMs)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(config.getUrl())
                .withUsername(config.getUsername())
                .withPassword(config.getPassword())
                .withDriverName(config.getDriverClass())
                .build();

        return JdbcSink.<T>sink(
                sql,
                statementBuilder,
                executionOptions,
                connectionOptions
        );
    }

    /**
     * 创建异步MySQL查询函数
     */
    public static <IN, OUT> RichAsyncFunction<IN, OUT> createAsyncQueryFunction(
            String dsName,
            AsyncQueryHandler<IN, OUT> queryHandler,
            int timeoutMs) {
        return new AsyncMySQLQueryFunction<>(dsName, queryHandler, timeoutMs);
    }

    /**
     * 将DataStream转换为异步查询DataStream
     */
    public static <IN, OUT> DataStream<OUT> createAsyncDataStream(
            DataStream<IN> inputStream,
            String dsName,
            AsyncQueryHandler<IN, OUT> queryHandler,
            int timeoutMs,
            int capacity) {

        RichAsyncFunction<IN, OUT> asyncFunction = createAsyncQueryFunction(dsName, queryHandler, timeoutMs);

        return AsyncDataStream.orderedWait(
                inputStream,
                asyncFunction,
                timeoutMs,
                TimeUnit.MILLISECONDS,
                capacity
        );
    }

    /**
     * 将DataStream转换为异步查询DataStream（使用默认参数）
     */
    public static <IN, OUT> DataStream<OUT> createAsyncDataStream(
            DataStream<IN> inputStream,
            String dsName,
            AsyncQueryHandler<IN, OUT> queryHandler) {
        return createAsyncDataStream(inputStream, dsName, queryHandler, DEFAULT_ASYNC_TIMEOUT, DEFAULT_ASYNC_CAPACITY);
    }

    /**
     * 查询处理器接口
     */
    public interface AsyncQueryHandler<IN, OUT> {
        OUT process(IN input, Connection connection) throws SQLException;
    }

    /**
     * 结果集提取器接口
     */
    public interface ResultSetExtractor<T> {
        T extract(ResultSet resultSet) throws SQLException;
    }

    /**
     * 异步MySQL查询函数实现
     */
    private static class AsyncMySQLQueryFunction<IN, OUT> extends RichAsyncFunction<IN, OUT> {
        private final String dsName;
        private final AsyncQueryHandler<IN, OUT> queryHandler;
        private final int timeoutMs;
        private transient MySQLUtil mysqlUtil;

        public AsyncMySQLQueryFunction(String dsName, AsyncQueryHandler<IN, OUT> queryHandler, int timeoutMs) {
            this.dsName = dsName;
            this.queryHandler = queryHandler;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 为每个并行实例创建独立的MySQLUtil实例
            mysqlUtil = MySQLUtil.createNewInstance();
        }

        @Override
        public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
            // 使用MySQLUtil的异步执行能力
            mysqlUtil.executeAsyncInternal(dsName, () -> {
                try (Connection connection = mysqlUtil.getConnectionInternal(dsName)) {
                    return queryHandler.process(input, connection);
                } catch (SQLException e) {
                    throw new RuntimeException("MySQL查询失败", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).thenAccept(result -> {
                if (result != null) {
                    resultFuture.complete(Collections.singletonList(result));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            }).exceptionally(ex -> {
                LOG.error("异步查询失败", ex);
                resultFuture.complete(Collections.emptyList());
                return null;
            });
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (mysqlUtil != null) {
                mysqlUtil.shutdownInternal();
            }
        }
    }

    /**
     * 创建默认的Row结果提取器
     */
    public static ResultSetExtractor<Row> createDefaultRowExtractor(RowTypeInfo rowTypeInfo) {
        return resultSet -> {
            int fieldCount = rowTypeInfo.getArity();
            Row row = new Row(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                Object value = resultSet.getObject(i + 1);
                // 通用类型转换处理
                value = convertDatabaseValue(value);
                row.setField(i, value);
            }
            return row;
        };
    }

    /**
     * 创建Flink MySQL Source (轮询读取) - 返回Row类型
     * 适用于需要定期查询数据库获取最新数据的场景
     * 
     * @param env 流执行环境
     * @param dsName 数据源名称
     * @param query SQL查询语句
     * @param fetchSize 每次查询的批量大小
     * @param pollingIntervalMs 轮询间隔(毫秒)
     * @return Row类型的数据流
     */
    public static DataStream<Row> createJdbcSourceWithPolling(
            StreamExecutionEnvironment env,
            String dsName,
            String query,
            int fetchSize,
            long pollingIntervalMs) {

        return env.addSource(new SourceFunction<Row>() {
            private volatile boolean isRunning = true;
            private transient MySQLUtil mysqlUtil;

            @Override
            public void run(SourceFunction.SourceContext<Row> ctx) throws Exception {
                while (isRunning) {
                    try (Connection conn = MySQLUtil.getConnection(dsName);
                         PreparedStatement stmt = conn.prepareStatement(query);
                         ResultSet rs = stmt.executeQuery()) {
                        
                        // 设置每次查询的批量大小
                        stmt.setFetchSize(fetchSize);
                        
                        // 提取结果集
                        while (rs.next() && isRunning) {
                            int columnCount = rs.getMetaData().getColumnCount();
                            Row row = new Row(columnCount);
                            for (int i = 0; i < columnCount; i++) {
                                Object value = rs.getObject(i + 1);
                                // 通用类型转换处理
                                value = convertDatabaseValue(value);
                                row.setField(i, value);
                            }
                            
                            // 发送数据到下游
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collect(row);
                            }
                        }
                    } catch (SQLException e) {
                        LOG.error("轮询MySQL查询失败", e);
                        // 短暂等待后重试
                        if (isRunning) {
                            Thread.sleep(5000);
                        }
                    } catch (ClassNotFoundException e) {
                        LOG.error("加载MySQL驱动失败", e);
                        // 短暂等待后重试
                        if (isRunning) {
                            Thread.sleep(5000);
                        }
                    }
                    
                    // 等待下一次轮询
                    if (isRunning) {
                        LOG.info("完成本次MySQL轮询，等待 {} 毫秒后开始下一次轮询", pollingIntervalMs);
                        try {
                            Thread.sleep(pollingIntervalMs);
                            LOG.info("开始新一轮MySQL轮询");
                        } catch (InterruptedException e) {
                            LOG.info("轮询线程被中断，准备退出轮询");
                            // 恢复中断状态
                            Thread.currentThread().interrupt();
                            // 退出轮询循环
                            isRunning = false;
                        }
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, "MySQL Polling Source");
    }

    /**
     * 创建Flink MySQL Source (轮询读取) - 返回Row类型（使用默认参数）
     */
    public static DataStream<Row> createJdbcSourceWithPolling(
            StreamExecutionEnvironment env,
            String dsName,
            String query,
            long pollingIntervalMs) {
        return createJdbcSourceWithPolling(env, dsName, query, 1000, pollingIntervalMs);
    }
//
//    /**
//     * 创建Flink MySQL Source (CDC实时捕获) - 返回JSON字符串类型
//     * 适用于需要实时捕获数据库表变化（INSERT/UPDATE/DELETE）的场景
//     * 注意：需要确保MySQL数据库已开启binlog，且格式为ROW格式
//     *
//     * @param env 流执行环境
//     * @param dsName 数据源名称
//     * @param databaseName 数据库名称
//     * @param tableName 表名称，支持正则表达式，如：`order_.*`
//     * @param startupOption 启动选项（从快照开始/从最新位置开始/从特定时间开始等）
//     * @return JSON字符串类型的数据流，包含变更事件的完整信息
//     */
//    public static DataStream<String> createJdbcSourceWithCdc(
//            StreamExecutionEnvironment env,
//            String dsName,
//            String databaseName,
//            String tableName,
//            StartupOptions startupOption) {
//
//        MySQLUtil.MySQLConfig config = MySQLUtil.getConfig(dsName);
//
//        // 解析JDBC URL获取主机和端口
//        String host = config.getUrl().split("//")[1].split(":")[0];
//        String port = config.getUrl().split(":")[2].split("/")[0];
//
//        // 创建MySQL CDC源
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname(host)
//                .port(Integer.parseInt(port))
//                .databaseList(databaseName) // 监控的数据库
//                .tableList(databaseName + "." + tableName) // 监控的表，格式：数据库名.表名
//                .username(config.getUsername())
//                .password(config.getPassword())
//                .startupOptions(startupOption) // 启动选项
//                .deserializer(new JsonDebeziumDeserializationSchema()) // JSON格式反序列化
//                .build();
//
//        // 创建数据流
//        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
//    }
//
//    /**
//     * 创建Flink MySQL Source (CDC实时捕获) - 返回JSON字符串类型（使用默认启动选项：从最新位置开始）
//     *getConnectionInternal
//     * @param env 流执行环境
//     * @param dsName 数据源名称
//     * @param databaseName 数据库名称
//     * @param tableName 表名称，支持正则表达式
//     * @return JSON字符串类型的数据流
//     */
//    public static DataStream<String> createJdbcSourceWithCdc(
//            StreamExecutionEnvironment env,
//            String dsName,
//            String databaseName,
//            String tableName) {
//        return createJdbcSourceWithCdc(env, dsName, databaseName, tableName, StartupOptions.latest());
//    }
}

