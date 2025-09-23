package com.gosh.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.gosh.cons.CommonConstants;


public class KafkaEnvUtil {
    /**
     * 从属性文件加载配置
     */
    public static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = KafkaEnvUtil.class.getClassLoader().getResourceAsStream(CommonConstants.KAFKA_DEFAULT_CONF)) {
            if (input == null) {
                throw new RuntimeException("无法找到配置文件: " + CommonConstants.KAFKA_DEFAULT_CONF);
            }
            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败: " + CommonConstants.KAFKA_DEFAULT_CONF, e);
        }
        return props;

    }

    /**
     * 创建 Kafka 源 - 修正版本
     */
    public static KafkaSource<String> createKafkaSource(Properties props,String topic) {
        String bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        String groupId = props.getProperty("group.id", "flink-kafka-consumer-group");
        String offsetReset = props.getProperty("auto.offset.reset", "earliest");
        props.setProperty("input.topic",topic);

        // 创建只包含必要配置的属性对象
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", bootstrapServers);
        kafkaProps.put("group.id", groupId);

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets("latest".equals(offsetReset) ?
                        OffsetsInitializer.earliest() : OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setProperties(kafkaProps)
                .build();
    }

    /**
     * 创建 Kafka 汇
     */
    private static KafkaSink<String> createKafkaSink(Properties props,String topic) {
        String bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("output.topic",topic);

        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }


    public static void main(String[] args) throws Exception {
        // 加载 Kafka 配置
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

        Properties kafkaProps = loadProperties();
        System.out.println("");

        // 创建 Kafka 源
        KafkaSource<String> kafkaSource = createKafkaSource(kafkaProps,"flink-input-topic");
        env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source")
                .print();
        env.execute("kafka job");


    }

}
