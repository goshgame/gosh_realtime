package com.gosh.util;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
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
     * 创建Flink MySQL Source (批量读取)
     * 适用于全量或分页读取MySQL表数据
     */
    public static <T> DataStream<T> createJdbcSource(
            StreamExecutionEnvironment env,
            String dsName,
            String query,
            int fetchSize) {

        MySQLUtil.MySQLConfig config = MySQLUtil.getConfig(dsName);

        // 构建JdbcSource，确保泛型类型正确传递
        JdbcSource<T> jdbcSource = JdbcSource.<T>builder()
                .setDBUrl(config.getUrl())
                .setUsername(config.getUsername())
                .setPassword(config.getPassword())
                .setSql(query)
                .setResultSetFetchSize(fetchSize)
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
                row.setField(i, resultSet.getObject(i + 1));
            }
            return row;
        };
    }
}

