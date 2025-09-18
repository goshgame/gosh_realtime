//package com.gosh;
//
//import com.gosh.serial.ProtobufSerializer;
//import com.gosh.serial.impl.GenericProtobufSerializer;
//import com.gosh.util.RedisUtil;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class ProtobufRedisSinkDemo {
//    public static void main(String[] args) throws Exception {
//        // 初始化Flink执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 创建测试用的Protobuf数据流
//        DataStream<UserMessageOuterClass.UserMessage> userStream = env.fromElements(
//                UserMessageOuterClass.UserMessage.newBuilder().setId(1).setName("Alice").setAge(30).build(),
//                UserMessageOuterClass.UserMessage.newBuilder().setId(2).setName("Bob").setAge(25).build(),
//                UserMessageOuterClass.UserMessage.newBuilder().setId(3).setName("Charlie").setAge(35).build()
//        );
//
//        // 创建Protobuf序列化器
//        ProtobufSerializer<UserMessageOuterClass.UserMessage> serializer = new GenericProtobufSerializer<>(UserMessageOuterClass.UserMessage.class);
//
//        // 添加Protobuf Redis Sink
//        RedisUtil.addRedisProtobufSink(userStream, serializer);
//
//        // 执行Flink作业
//        env.execute("Protobuf Redis Sink Demo");
//    }
//}
