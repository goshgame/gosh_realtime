package com.gosh.serial;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import java.nio.ByteBuffer;

/**
 * 键为String类型，值为byte[]类型的编码解码器
 */
public class StringByteArrayCodec implements RedisCodec<String, byte[]> {
    private final StringCodec stringCodec = new StringCodec();

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return stringCodec.decodeKey(bytes);
    }

    @Override
    public byte[] decodeValue(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        byte[] value = new byte[bytes.remaining()];
        bytes.get(value);
        return value;
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return stringCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(byte[] value) {
        if (value == null) {
            return ByteBuffer.wrap(new byte[0]);
        }
        return ByteBuffer.wrap(value);
    }
}