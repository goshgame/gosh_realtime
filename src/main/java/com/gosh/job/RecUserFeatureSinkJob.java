package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeatureOuterClass;
import com.gosh.util.RedisUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.function.Function;

public class RecUserFeatureSinkJob {
    //前缀
    private static String PREFIX = "rec:user_feature:";
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 构建测试用的RecUserFeature实体
        RecFeatureOuterClass.RecFeature testFeature = RecFeatureOuterClass.RecFeature.newBuilder()
                .setKey("12345489")
                .setResutls("{'user_foru_explive_cnt_24h':110,'user_foru_joinlive_cnt_24h':3,'user_quitlive_3latest':'live_678:300|live_910:151'}")
                .build();

        // 3. 打印测试数据信息（补全用户代码）
        System.out.println("测试数据: " + testFeature.toString());
        System.out.println("测试数据字节长度: " + testFeature.toByteArray().length);

        // 4. 将RecUserFeature转换为字节数组（protobuf序列化），并创建DataStream<byte[]>
        DataStream<byte[]> dataStream = env.fromElements(
                testFeature.toByteArray() // 序列化测试对象为字节数组
        );


        // 5. 配置Redis参数
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());

        // 6. 定义protobuf解析器和key提取逻辑
        Class<RecFeatureOuterClass.RecFeature> protoClass = RecFeatureOuterClass.RecFeature.class;
        // 确保keyExtractor是可序列化的（显式类或Flink的Function）
        Function<RecFeatureOuterClass.RecFeature, String> keyExtractor = new UserKeyExtractor();


        // 7. 添加Redis Sink（使用自定义的protobuf参数）
        RedisUtil.addRedisSink(
                dataStream,
                redisConfig,
                false, // 异步写入
                100,  // 批量大小
                protoClass,
                keyExtractor
        );

        // 8. 执行任务
        env.execute("Protobuf to Redis Example");
    }

    //设置 key值
    private static class UserKeyExtractor implements Function<RecFeatureOuterClass.RecFeature, String>, Serializable {
        @Override
        public String apply(RecFeatureOuterClass.RecFeature feature) {
            return PREFIX + feature.getKey();
        }
    }
}
