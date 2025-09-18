//package com.gosh;
//
//import com.gosh.serial.ProtobufSerializer;
//import com.gosh.serial.impl.GenericProtobufSerializer;
//import com.gosh.util.RedisUtil;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class ProtobufRedisSourceDemo {
//    public static void main(String[] args) throws Exception {
//        // 初始化Flink执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 创建Protobuf序列化器
//        ProtobufSerializer<UserMessageOuterClass.UserMessage> serializer = new GenericProtobufSerializer<>(UserMessageOuterClass.UserMessage.class);
//
//        // 创建Protobuf Redis Source
//        DataStream<UserMessageOuterClass.UserMessage> protobufStream = RedisUtil.createRedisProtobufSource(env, serializer);
//
//        // 处理数据 - 打印用户信息
//        protobufStream.map(user ->
//                String.format("User: %s, Age: %d", user.getName(), user.getAge())
//        ).print("Protobuf Data: ");
//
//        // 执行Flink作业
//        env.execute("Protobuf Redis Source Demo");
//    }
//}