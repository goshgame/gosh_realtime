package com.gosh.serial;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;

/**
 * Protobuf 序列化器接口
 */
public interface ProtobufSerializer<T extends Message> {

    /**
     * 将 Protobuf 消息序列化为字节数组
     */
    byte[] serialize(Message message) throws IOException;

    /**
     * 将字节数组反序列化为 Protobuf 消息
     */
    T deserialize(byte[] data) throws IOException;

    /**
     * 获取 Protobuf 消息的解析器
     */
    Parser<T> getParser();

    /**
     * 获取 Protobuf 消息的类型
     */
    Class<T> getMessageType();
}