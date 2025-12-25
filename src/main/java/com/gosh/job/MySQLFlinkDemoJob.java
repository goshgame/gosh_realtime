package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.util.FlinkEnvUtil;
//import com.gosh.util.FlinkMonitorUtil;
import com.gosh.util.KafkaEnvUtil;
import com.gosh.util.MySQLFlinkUtil;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * MySQL Flink 集成示例作业
 * 演示：
 * 1. 使用MySQLFlinkUtil的createJdbcSource读取MySQL数据并写入RedisSink
 * 2. 从Kafka读取数据并通过MySQLFlinkUtil的createJdbcSink写入MySQL
 */
public class MySQLFlinkDemoJob {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLFlinkDemoJob.class);
    private static final String JOB_NAME = "MySQL-Flink-Demo-Job";

    public static void main(String[] args) throws Exception {
        // 初始化Flink执行环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();
        
        // 创建监控实例
        //FlinkMonitorUtil.create(JOB_NAME, "MySQLDemoOperator");

        // 第一部分：MySQL Source → Redis Sink
        // 实现MySQL读取数据，统计用户ID出现次数并写入Redis
        mysqlToRedisProcessing(env);

        // 第二部分：Kafka Source → MySQL Sink
        // 从Kafka读取数据，统计用户ID出现次数并写入MySQL
       //kafkaToMySQLProcessing(env);

        // 执行作业并监控
        //FlinkMonitorUtil.executeWithMonitor(env, JOB_NAME);
        env.execute(JOB_NAME);
    }

    /**
     * MySQL Source → Redis Sink 处理逻辑
     * 从MySQL读取数据，使用SQL "select user_id, count(*) cnt from dual group by user_id"
     */
    private static void mysqlToRedisProcessing(StreamExecutionEnvironment env) throws Exception {
        LOG.info("开始处理 MySQL Source → Redis Sink");

        // 定义查询SQL，实现用户ID统计
        String query = "SELECT uid user_id,count(*) cnt FROM agency_member WHERE anchor_type IN (11, 15) group by uid";

        // 使用MySQLFlinkUtil创建JdbcSource
        // 指定 dbName，默认使用配置文件中的数据库名称
        DataStream<Row> mysqlStream = MySQLFlinkUtil.createJdbcSourceWithPolling(
                env,  // Flink执行环境
                "db1", // 数据源名称，需要在配置文件中定义
                "gosh", // 数据库名称
                query, // SQL查询语句
                1000,  // 批量获取大小
                50000  // 获取间隔
        );  
        //不指定 dbName，默认使用配置文件中的数据库名称
        // DataStream<Row> mysqlStream = MySQLFlinkUtil.createJdbcSourceWithPolling(
        //         env,  // Flink执行环境
        //         "db1", // 数据源名称，需要在配置文件中定义
        //         query, // SQL查询语句
        //         1000,  // 批量获取大小
        //         50000  // 获取间隔
        // );

        // 转换数据格式为Redis需要的Tuple2<String, byte[]>
        DataStream<Tuple2<String, byte[]>> redisDataStream = mysqlStream
                .map(row -> {
                    // 提取用户ID和计数
                    String userId = row.getField(0).toString();
                    Long count = (Long) row.getField(1);
                    
                    // 构建Redis key和value
                    String redisKey = "user:count:" + userId;
                    byte[] valueBytes = count.toString().getBytes(StandardCharsets.UTF_8);
                    
                    return new Tuple2<>(redisKey, valueBytes);
                })
                .returns(Types.TUPLE(Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE)))
                .name("MySQL数据转换为Redis格式");
        
        redisDataStream.print();
        // 创建Redis配置
//        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
//        redisConfig.setCommand("SET"); // 使用SET命令
//        redisConfig.setTtl(3600); // 设置1小时过期时间
//
//        // 添加Redis Sink
//        RedisUtil.addRedisSink(
//                redisDataStream,
//                redisConfig,
//                true,  // 异步写入
//                100    // 批量大小
//        );

        LOG.info("MySQL Source → Redis Sink 处理逻辑配置完成");
    }

    /**
     * Kafka Source → MySQL Sink 处理逻辑
     * 从Kafka读取数据，统计用户ID出现次数并写入MySQL
     */
    private static void kafkaToMySQLProcessing(StreamExecutionEnvironment env) throws Exception {
        LOG.info("开始处理 Kafka Source → MySQL Sink");

        // 加载Kafka配置并创建KafkaSource
        Properties kafkaProps = KafkaEnvUtil.loadProperties();
        KafkaSource<String> kafkaSource = KafkaEnvUtil.createKafkaSource(
                kafkaProps,
                "user_events"  // Kafka主题
        );

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 解析Kafka消息，提取用户ID并进行聚合
        // 注意：这里简化了处理逻辑，实际应用中需要根据消息格式正确解析
        DataStream<Tuple2<String, Long>> aggregatedStream = kafkaStream
                .map(message -> {
                    // 假设消息格式为JSON，包含user_id字段
                    // 这里简化处理，实际应用中应使用JSON解析器
                    String userId = extractUserIdFromMessage(message);
                    return new Tuple2<>(userId, 1L);
                })
                .name("提取用户ID")
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .name("按用户ID聚合计数");

        // 定义MySQL插入SQL
        String insertSql = "INSERT INTO user_counts (user_id, count) VALUES (?, ?) " +
                           "ON DUPLICATE KEY UPDATE count = VALUES(count)";

        // 创建JdbcStatementBuilder，用于设置SQL参数
        JdbcStatementBuilder<Tuple2<String, Long>> statementBuilder = new JdbcStatementBuilder<Tuple2<String, Long>>() {
            @Override
            public void accept(PreparedStatement preparedStatement, Tuple2<String, Long> data) throws SQLException {
                preparedStatement.setString(1, data.f0); // user_id
                preparedStatement.setLong(2, data.f1);  // count
            }
        };

        // 使用MySQLFlinkUtil创建JdbcSink
        aggregatedStream.addSink(
                MySQLFlinkUtil.createJdbcSink(
                        "db1",          // 数据源名称
                        insertSql,       // SQL语句
                        statementBuilder, // 参数设置器
                        100,            // 批量大小
                        1000            // 批量间隔(毫秒)
                )
        ).name("MySQL Sink");

        LOG.info("Kafka Source → MySQL Sink 处理逻辑配置完成");
    }

    /**
     * 从消息中提取用户ID
     * 这里简化实现，实际应用中需要根据消息格式正确解析
     */
    private static String extractUserIdFromMessage(String message) {
        try {
            // 假设消息格式为JSON，这里使用简单的字符串处理
            // 实际应用中应使用JSON解析库，如Jackson或Gson
            if (message.contains("user_id")) {
                int start = message.indexOf("user_id") + 9;
                int end = message.indexOf(",", start);
                if (end == -1) {
                    end = message.indexOf("}", start);
                }
                return message.substring(start, end).replaceAll("\"", "").trim();
            }
        } catch (Exception e) {
            LOG.error("解析用户ID失败: {}", message, e);
        }
        // 默认返回未知用户ID，避免数据丢失
        return "unknown";
    }
}