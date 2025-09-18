package com.gosh;

import com.gosh.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BasicRedisSourceDemo {
    public static void main(String[] args) throws Exception {
        // 初始化Flink执行环境
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建Redis Source（字符串类型）
        //DataStream<String> redisDataStream = RedisUtil.createRedisSource(env);
        Tuple2<StreamExecutionEnvironment, DataStream> redisSourceWithEnv = RedisUtil.createRedisSourceWithEnv();
        StreamExecutionEnvironment env = redisSourceWithEnv.f0;
        DataStream redisDataStream = redisSourceWithEnv.f1;
        int id = redisDataStream.getId();
        System.out.println("DataStream ID: " + id);

        // 打印获取到的数据
        redisDataStream.print("Redis Source Data: ");
        RedisUtil.addRedisSink(redisDataStream);

        // 执行Flink作业
        env.execute("Basic Redis Source Demo");
    }
}