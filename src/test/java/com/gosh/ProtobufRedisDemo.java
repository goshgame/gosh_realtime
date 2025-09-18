//package com.gosh;
//
//import com.gosh.entity.RecUserFeatureOuterClass.RecUserFeature;
//import com.gosh.config.RedisConfig;
//import com.gosh.util.RedisUtil;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//import java.util.function.Function;
//
///**
// * ProtobufRedisUtil 工具类使用示例
// * 功能：从 Redis 读取 RecUserFeature 数据，简单处理后写回 Redis
// */
//public class ProtobufRedisDemo {
//
//    public static void main(String[] args) throws Exception {
//        // 1. 初始化 Flink 执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 本地调试可设置并行度为 1
//        env.setParallelism(1);
//
//        // 2. 加载 Redis 配置（从配置文件或手动构建）
//        Properties redisProps = RedisUtil.loadProperties(); // 从默认配置文件加载
//        RedisConfig redisConfig = RedisConfig.fromProperties(redisProps);
//        // 自定义配置补充（根据业务场景调整）
//        redisConfig.setKeyPattern("user:feature:*"); // 键匹配模式
//        redisConfig.setCommand("SET"); // 写入命令
//        redisConfig.setValueType("string"); // 数据类型
//        redisConfig.setSerializerType("protobuf"); // 序列化类型
//        redisConfig.setProtobufMessageType("com.gosh.entity.RecUserFeatureOuterClass.RecUserFeature"); // Protobuf类全路径
//
//        // 3. 定义 Protobuf 反序列化器（从字节数组解析为 RecUserFeature）
//        Function<byte[], RecUserFeature> deserializer = bytes -> {
//            try {
//                return RecUserFeature.parseFrom(bytes);
//            } catch (Exception e) {
//                throw new RuntimeException("Protobuf反序列化失败", e);
//            }
//        };
//
//        // 4. 使用 ProtobufRedisUtil 创建 Redis Source（异步读取）
//        ProtobufRedisSource<RecUserFeature> featureSource = ProtobufRedisUtil.createProtobufSource(
//                redisConfig,
//                true, // 启用异步读取
//                5000, // 轮询间隔 5 秒
//                deserializer
//        );
//        DataStream<RecUserFeature> featureStream = env.addSource(featureSource)
//                .name("RecUserFeature-Redis-Source");
//
//        // 5. 数据处理（示例：打印数据并增加一个特征值）
//        DataStream<RecUserFeature> processedStream = featureStream
//                .map(feature -> {
//                    // 打印原始数据
//                    System.out.println("读取到用户特征：" + feature.getUid() +
//                            "，24h曝光次数：" + feature.getUserForuExpliveCnt24H());
//                    // 示例处理：增加曝光次数
//                    return RecUserFeature.newBuilder(feature)
//                            .setUserForuExpliveCnt24H(feature.getUserForuExpliveCnt24H() + 1)
//                            .build();
//                })
//                .name("Process-Feature-Stream");
//
//        // 6. 定义 Redis 键提取器（从 Protobuf 对象中提取存储键）
//        Function<RecUserFeature, String> keyExtractor = feature ->
//                "user:feature:" + feature.getUid(); // 保持与原始 KEY_PREFIX 一致
//
//        // 7. 使用 ProtobufRedisUtil 添加 Redis Sink（异步写入，批量处理）
//        ProtobufRedisUtil.addProtobufSink(
//                processedStream,
//                redisConfig,
//                true, // 启用异步写入
//                100, // 批量大小：每积累 100 条数据统一提交
//                keyExtractor
//        );
//
//        // 8. 执行 Flink 作业
//        env.execute("Protobuf-Redis-Flink-Demo");
//    }
//}