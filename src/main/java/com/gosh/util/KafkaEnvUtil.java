package com.gosh.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.gosh.entity.KafkaRawEvent;
import com.gosh.serial.KafkaRawEventDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        String offsetReset = props.getProperty("auto.offset.reset", "latest");
        props.setProperty("input.topic",topic);

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets("latest".equals(offsetReset) ?
                        OffsetsInitializer.latest() : OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setProperties(props)
                .build();
    }

    public static KafkaSource<KafkaRawEvent> createKafkaRawSource(Properties props,String topic) {
        String bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");
        String groupId = props.getProperty("group.id", "flink-kafka-consumer-group");
        String offsetReset = props.getProperty("auto.offset.reset", "latest");
        props.setProperty("input.topic",topic);


        return KafkaSource.<KafkaRawEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperties(props)
                .setStartingOffsets("latest".equals(offsetReset) ?
                        OffsetsInitializer.latest() : OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRawEventDeserializationSchema())
                .build();
    }


    /**
     * 创建 Kafka 汇
     */
    public static KafkaSink<String> createKafkaSink(Properties props,String topic) {
        String bootstrapServers = props.getProperty("bootstrap.servers", "localhost:9092");

        props.setProperty("output.topic",topic);

        Properties kafkaProps = new Properties();
        // 复制原props中的所有配置（而非仅复制2个参数）
        for (String key : props.stringPropertyNames()) {
            kafkaProps.setProperty(key, props.getProperty(key));
        }


        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(kafkaProps)
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
