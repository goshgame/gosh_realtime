package com.gosh.serial;

import com.gosh.entity.KafkaRawEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 自定义反序列化器，提取Kafka消息的原始信息并封装为KafkaRawEvent
 */
public class KafkaRawEventDeserializationSchema implements KafkaRecordDeserializationSchema<KafkaRawEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRawEventDeserializationSchema.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRawEvent> collector) throws IOException {
        KafkaRawEvent event = new KafkaRawEvent();
        try {
            // 提取消息Value（消息体）
            if (record.value() != null) {
                event.setMessage(new String(record.value(), StandardCharsets.UTF_8));
            } else {
                event.setMessage(""); // 空Value处理
            }

            // 提取元数据
            event.setTopic(record.topic()); // 消息所属Topic
            event.setTimestamp(record.timestamp()); // 消息时间戳
            event.setOffset(record.offset()); // 消息偏移量
            event.setPartition(record.partition()); // 消息所在分区

            LOG.debug("成功反序列化Kafka消息: {}", event);
            collector.collect(event);

        } catch (Exception e) {
            // 捕获异常，记录错误上下文（便于问题定位）
            LOG.error("反序列化Kafka消息失败！Topic: {}, Partition: {}, Offset: {}",
                    record.topic(), record.partition(), record.offset(), e);
            // 容错处理：忽略错误消息，避免任务崩溃（根据业务需求调整）
        }
    }

    @Override
    public TypeInformation<KafkaRawEvent> getProducedType() {
        return TypeInformation.of(KafkaRawEvent.class);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
    }
}
