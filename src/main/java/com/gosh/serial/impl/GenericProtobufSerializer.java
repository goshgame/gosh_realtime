package com.gosh.serial.impl;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.gosh.serial.ProtobufSerializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 通用 Protobuf 序列化器实现
 */
public class GenericProtobufSerializer<T extends Message> implements ProtobufSerializer<T> {

    private final Class<T> messageType;
    private final Parser<T> parser;

    public GenericProtobufSerializer(Class<T> messageType) {
        this.messageType = messageType;
        try {
            // 使用反射获取解析器
            Method parserMethod = messageType.getMethod("parser");
            this.parser = (Parser<T>) parserMethod.invoke(null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to get parser for message type: " + messageType.getName(), e);
        }
    }

    @Override
    public byte[] serialize(Message message) throws IOException {
        return message.toByteArray();
    }

    @Override
    public T deserialize(byte[] data) throws IOException {
        try {
            return parser.parseFrom(data);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize protobuf message", e);
        }
    }

    @Override
    public Parser<T> getParser() {
        return parser;
    }

    @Override
    public Class<T> getMessageType() {
        return messageType;
    }
}