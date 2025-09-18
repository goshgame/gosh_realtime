//package com.gosh.util;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
//import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
//import org.apache.flink.connector.jdbc.source.JdbcSource;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.*;
//import java.util.function.Supplier;
//
//import static org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy.build;
//
///**
// * Flink 1.20 MySQL 连接器工具类
// * 扩展MySQLUtil，增加Flink Source/Sink支持及异步处理
// */
//public class MySQLFlinkUtil extends MySQLUtil {
//    private static final Logger LOG = LoggerFactory.getLogger(MySQLFlinkUtil.class);
//    private static final int ASYNC_TIMEOUT = 5000; // 异步操作超时时间(ms)
//    private static final int ASYNC_CAPACITY = 100; // 异步队列容量
//
//    /**
//     * 创建Flink MySQL Source (批量读取)
//     * 适用于全量或分页读取MySQL表数据
//     */
//    public static DataStream<Row> createJdbcSource(
//            StreamExecutionEnvironment env,
//            String dsName,
//            String query,
//            RowTypeInfo rowTypeInfo,
//            int fetchSize) {
//
//        MySQLConfig config = getConfig(dsName);
//
//        JdbcSource<Row> jdbcSource = JdbcSource.<Row>builder()
//                //.setDrivername("com.mysql.cj.jdbc.Driver") // 设置数据库驱动
//                .setDBUrl(config.getUrl()) // 设置数据库URL
//                .setUsername(config.getUsername()) // 设置数据库用户名
//                .setPassword(config.getPassword()) // 设置数据库密码
//                .setSql(query) // 设置查询语句
//                // 可以设置其他参数，如批大小等
//                .setResultSetFetchSize(fetchSize) // 设置每次查询的行数
//                //.setResultSetType(ResultSetType.FORWARD_ONLY) // 设置结果集类型
//                //.setStatementType(StatementType.FORWARD_ONLY) // 设置语句类型
//                //.setStatementBehavior(StatementBehaviorOptions.builder().build()) // 设置语句行为选项
//                // .setExecutionOptions(JdbcExecutionOptions.builder().withBatchSize(1000).build()) // 设置执行选项，例如批处理大小
//                // .setConnectionOptions(JdbcConnectionOptions.builder().withUrl("jdbc:mysql://localhost:3306/yourdatabase").build()) // 设置连接选项
//                // 可以使用自定义的ConnectionProvider，例如使用HikariCP连接池
//                // .setConnectionProvider(new HikariConnectionProvider()) // 使用HikariCP连接池
//                // 或者使用内置的DriverConnectionFactory，例如：
//                // .setConnectionFactory(new DriverConnectionFactory(new JdbcUrlOptions("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/yourdatabase"), "yourusername", "yourpassword"))
//                // 指定返回的TypeInformation，例如使用RowData类型：
//                // .setRowTypeInfo(TypeInformationUtil.toTypeInfo(TypesFactoryUtil.getRowTypeInfo())) // 对于复杂的类型可能需要更详细的配置
//                // 或者使用更具体的类型信息，例如：
//                // ==.setRowTypeInfo(TypeInformationUtil.toTypeInfo(TypesFactoryUtil.<YourCustomType>getRowTypeInfo())) // 替换YourCustomType为你的实际类型
//                // 注意：如果你的表结构较为复杂，可能需要自定义RowTypeInfo或使用Table API来处理。
//                // 构建并返回JdbcSource对象：
//                .setResultExtractor(new ResultExtractor<Row>() {
//                    @Override
//                    public Row extract(ResultSet resultSet) throws SQLException {
//                        return null;
//                    }
//                })
//                .build();
//
//
//        // 创建 JdbcConnectionOptions
////        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
////                .withUrl(config.getUrl())
////                .withUsername(config.getUsername())
////                .withPassword(config.getPassword())
////                .withDriverName(config.getDriverClass())
////                .build();
////
////        // 创建 ConnectionProvider
////        SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(connectionOptions);
////        // 构建JdbcSource
////        JdbcSource<Row> jdbcSource = JdbcSource.<Row>builder()
////                .setConnectionProvider(connectionProvider)
////                .setSql(query)
////                .setResultSetFetchSize(fetchSize)
////                .build();
////
////
////        // 显式指定类型信息创建数据源
//        return env.fromSource(jdbcSource,
//                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
//                        "MySQL Source")
//                .returns(rowTypeInfo);
//    }
//
//    /**
//     * 创建Flink MySQL Sink (批量写入)
//     * 支持异步批量提交
//     */
//    public static <T> void addJdbcSink(
//            DataStream<T> dataStream,
//            String dsName,
//            String insertSql,
//            int batchSize,
//            long batchIntervalMs,
//            JdbcStatementBuilder<T> statementBuilder) {
//
//        MySQLConfig config = getConfig(dsName);
//
//        SinkFunction<T> sink = JdbcSink.sink(
//                insertSql,
//                statementBuilder,
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(batchSize)
//                        .withBatchIntervalMs(batchIntervalMs)
//                        .withMaxRetries(3)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl(config.getUrl())
//                        .withUsername(config.getUsername())
//                        .withPassword(config.getPassword())
//                        .withDriverName(config.getDriverClass())
//                        .build()
//        );
//
//        dataStream.addSink(sink).name("MySQL Sink");
//    }
//
//    /**
//     * 创建Row类型的JdbcSink
//     */
//    public static void addRowJdbcSink(
//            DataStream<Row> dataStream,
//            String dsName,
//            String insertSql,
//            int batchSize,
//            long batchIntervalMs) {
//
//        addJdbcSink(
//                dataStream,
//                dsName,
//                insertSql,
//                batchSize,
//                batchIntervalMs,
//                (statement, row) -> {
//                    // 绑定参数
//                    for (int i = 0; i < row.getArity(); i++) {
//                        statement.setObject(i + 1, row.getField(i));
//                    }
//                }
//        );
//    }
//
//    /**
//     * 异步查询MySQL (基于Flink Async I/O)
//     * 适用于流处理中关联MySQL维度表
//     */
//    public static DataStream<Row> asyncQuery(
//            DataStream<Row> inputStream,
//            String dsName,
//            String queryTemplate,
//            int fieldIndex,
//            RowTypeInfo resultTypeInfo) {
//
//        ExecutorService executor = getExecutorService(dsName);
//
//        return AsyncDataStream.unorderedWait(
//                inputStream,
//                new RichAsyncFunction<Row, Row>() {
//                    private transient Connection connection;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
//                        MySQLConfig config = getConfig(dsName);
//                        Class.forName(config.getDriverClass());
//                        connection = DriverManager.getConnection(
//                                config.getUrl(),
//                                config.getUsername(),
//                                config.getPassword()
//                        );
//                        connection.setAutoCommit(false);
//                    }
//
//                    @Override
//                    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
//                        Object queryParam = input.getField(fieldIndex);
//                        String query = String.format(queryTemplate, queryParam);
//
//                        CompletableFuture.supplyAsync((Supplier<List<Row>>) () -> {
//                            try (PreparedStatement stmt = connection.prepareStatement(query)) {
//                                ResultSet rs = stmt.executeQuery();
//                                return convertResultSet(rs, resultTypeInfo);
//                            } catch (SQLException e) {
//                                LOG.error("异步查询失败: " + query, e);
//                                return Collections.emptyList();
//                            }
//                        }, executor).thenAccept(resultFuture::complete);
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        if (connection != null) {
//                            connection.close();
//                        }
//                        super.close();
//                    }
//                },
//                ASYNC_TIMEOUT,
//                TimeUnit.MILLISECONDS,
//                ASYNC_CAPACITY
//        );
//    }
//
//    /**
//     * 异步写入MySQL (基于线程池)
//     */
//    public static void asyncSink(
//            DataStream<Row> dataStream,
//            String dsName,
//            String insertSql) {
//
//        ExecutorService executor = getExecutorService(dsName);
//        dataStream.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public void flatMap(Row row, Collector<Row> collector) throws Exception {
//                CompletableFuture.runAsync(() -> {
//                    try (Connection conn = getConnection(dsName);
//                         PreparedStatement stmt = conn.prepareStatement(insertSql)) {
//
//                        for (int i = 0; i < row.getArity(); i++) {
//                            stmt.setObject(i + 1, row.getField(i));
//                        }
//                        stmt.executeUpdate();
//                        conn.commit();
//
//                    } catch (Exception e) {
//                        LOG.error("异步写入失败", e);
//                        try (Connection conn = getConnection(dsName)) {
//                            conn.rollback();
//                        } catch (SQLException ex) {
//                            LOG.error("回滚失败", ex);
//                        } catch (ClassNotFoundException ex) {
//                            throw new RuntimeException(ex);
//                        }
//                    }
//                }, executor).exceptionally(ex -> {
//                    LOG.error("异步写入任务异常", ex);
//                    return null;
//                });
//                collector.collect(row);
//            }
//        });
//    }
//
//    /**
//     * 获取数据源对应的线程池
//     */
//    private static ExecutorService getExecutorService(String dsName) {
//        MySQLUtil.initialize();
//        ExecutorService executor =executorServices.get(dsName);
//        if (executor == null) {
//            throw new IllegalArgumentException("未找到数据源[" + dsName + "]的线程池配置");
//        }
//        return executor;
//    }
//
//    /**
//     * 将ResultSet转换为Row列表
//     */
//    private static List<Row> convertResultSet(ResultSet rs, RowTypeInfo rowTypeInfo) throws SQLException {
//        List<Row> result = new ArrayList<>();
//        ResultSetMetaData metaData = rs.getMetaData();
//        int columnCount = metaData.getColumnCount();
//
//        while (rs.next()) {
//            Row row = new Row(columnCount);
//            for (int i = 0; i < columnCount; i++) {
//                row.setField(i, rs.getObject(i + 1));
//            }
//            result.add(row);
//        }
//        return result;
//    }
//
//    /**
//     * 创建JDBC连接选项
//     */
//    private static JdbcConnectionOptions createJdbcConnectionOptions(MySQLConfig config) {
//        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(config.getUrl())
//                .withUsername(config.getUsername())
//                .withPassword(config.getPassword())
//                .withDriverName(config.getDriverClass())
//                .build();
//    }
//
//    /**
//     * 创建JDBC执行选项
//     */
//    private static JdbcExecutionOptions createJdbcExecutionOptions(int batchSize, long batchIntervalMs) {
//        return JdbcExecutionOptions.builder()
//                .withBatchSize(batchSize)
//                .withBatchIntervalMs(batchIntervalMs)
//                .withMaxRetries(3)
//                .build();
//    }
//
//    // 使用示例
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 1. 定义数据类型 (id, name, age)
//        TypeInformation<?>[] types = {
//                TypeInformation.of(Integer.class),
//                TypeInformation.of(String.class),
//                TypeInformation.of(Integer.class)
//        };
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, new String[]{"id", "name", "age"});
//
//        // 2. 创建MySQL Source (读取用户表)
//        DataStream<Row> userStream = createJdbcSource(
//                env,
//                "db1",
//                "SELECT id, name, age FROM user WHERE status = 1",
//                rowTypeInfo,
//                1000
//        );
//
//        // 3. 异步关联MySQL维度表 (查询用户等级)
//        DataStream<Row> enrichedStream = asyncQuery(
//                userStream,
//                "db2",
//                "SELECT level FROM user_level WHERE user_id = %s",
//                0,
//                new RowTypeInfo(new TypeInformation[]{TypeInformation.of(String.class)}, new String[]{"level"})
//        );
//
//        // 4. 异步写入结果到MySQL
//        asyncSink(
//                enrichedStream,
//                "db1",
//                "INSERT INTO user_stats (user_id, level, update_time) VALUES (?, ?, NOW())"
//        );
//
//        try {
//            env.execute("Flink MySQL Async Demo");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            shutdown(); // 关闭线程池
//        }
//    }
//}