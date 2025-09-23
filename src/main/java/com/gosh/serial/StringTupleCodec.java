package com.gosh.serial;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.apache.flink.api.java.tuple.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 键为String类型，值为Tuple<String, String>类型的编码解码器
 */
public class StringTupleCodec implements RedisCodec<String, Tuple2<String, byte[]>> {
    private final StringCodec stringCodec = new StringCodec();
    // 选择null字符作为分隔符（普通字符串通常不包含此字符）
    private static final byte SEPARATOR = '\0';

    @Override
    public String decodeKey(ByteBuffer bytes) {
        // 键仍为String，复用StringCodec的解码逻辑
        return stringCodec.decodeKey(bytes);
    }

    @Override
    public Tuple2<String, byte[]> decodeValue(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        // 直接获取原始字节数组，不转为String
        byte[] valueBytes = new byte[bytes.remaining()];
        bytes.get(valueBytes);
        // 直接返回原始字节数组作为Tuple2的value
        return new Tuple2<>("", valueBytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        // 键仍为String，复用StringCodec的编码逻辑
        return stringCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(Tuple2<String, byte[]> value) {
        if (value == null) {
            return ByteBuffer.wrap(new byte[0]);
        }
        // 从Tuple2中获取两个元素，处理null情况
        String v1 = value.f0 == null ? "" : value.f0;
        byte[] v2 = value.f1 == null ? "".getBytes() : value.f1;
        // 用分隔符拼接两个元素
//        String combined = v1 + new String(new byte[]{SEPARATOR}, StandardCharsets.UTF_8) + v2;
        byte[] combined = v2;
        // 转换为字节数组并包装为ByteBuffer
        return ByteBuffer.wrap(combined);
    }
}