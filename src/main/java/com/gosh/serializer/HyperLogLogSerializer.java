package com.gosh.serializer;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

public class HyperLogLogSerializer extends Serializer<HyperLogLog> {

    public static final HyperLogLogSerializer INSTANCE = new HyperLogLogSerializer();

    @Override
    public void write(Kryo kryo, Output output, HyperLogLog object) {
        if (object == null) {
            output.writeInt(-1);
        } else {
            try {
                byte[] bytes = object.getBytes();
                output.writeInt(bytes.length);
                output.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize HyperLogLog", e);
            }
        }
    }

    @Override
    public HyperLogLog read(Kryo kryo, Input input, Class<HyperLogLog> type) {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        try {
            return HyperLogLog.Builder.build(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize HyperLogLog", e);
        }
    }
}
