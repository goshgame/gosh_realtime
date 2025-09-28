package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * kafka 原始数据
 */
public class KafkaRawEvent {
    //消息时间戳
    @JsonProperty("message")
   private String message ;
   //消息时间戳
   @JsonProperty("timestamp")
    private long timestamp;
    //消息偏移量
    @JsonProperty("offset")
    private long offset;
    //分区号
    @JsonProperty("partition")
    private int partition ;
    @JsonProperty("topic")
    private String topic;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "KafkaRawEvent{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                ", offset=" + offset +
                ", partition=" + partition +
                ", topic='" + topic + '\'' +
                '}';
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}
