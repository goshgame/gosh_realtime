package com.gosh;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BasicRedisSinkDemo {
    public static void main(String[] args) throws Exception {
        // 初始化Flink执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建测试数据流
        DataStreamSource<String> dataStream = env.fromElements(
                "Hello Redis",
                "Flink Redis Integration",
                "Test Message 1",
                "Test Message 2"
        );

        // 添加Redis Sink
        //RedisUtil.addRedisSink(dataStream);

        // 执行Flink作业
        env.execute("Basic Redis Sink Demo");
    }
}