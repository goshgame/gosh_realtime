package com.gosh.job;

import com.gosh.config.RedisConfig;
import com.gosh.entity.RecFeatureDemoOuterClass;
import com.gosh.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
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
        RecFeatureDemoOuterClass.RecFeature testFeature = RecFeatureDemoOuterClass.RecFeature.newBuilder()
                .setKey("12345482")
                .setResutls("{'user_foru_explive_cnt_24h':114,'user_foru_joinlive_cnt_24h':3,'user_quitlive_3latest':'live_678:300|live_910:151'}")
                .build();

        // 3. 打印测试数据信息（补全用户代码）
        System.out.println("测试数据: " + testFeature.toString());
        // 提取Key和Results用于打印
        String key = new UserKeyExtractor().apply(testFeature);
        byte[] results = testFeature.getResutlsBytes().toByteArray();
        System.out.println("提取的Redis Key: " + key);
        System.out.println("提取的Redis Value: " + results);

        // 4. 将RecFeature转换为Tuple2<String, String>，并创建DataStream<Tuple2<String, String>>
        DataStream<Tuple2<String, byte[]>> dataStream = env.fromElements(
                new Tuple2<>(key, results)  // Key为第一个元素，results为第二个元素
        );




        // 5. 配置Redis参数
        RedisConfig redisConfig = RedisConfig.fromProperties(RedisUtil.loadProperties());
        // 增加ttl配置
        redisConfig.setTtl(1000);

        // 6. 定义protobuf解析器和key提取逻辑
        Class<RecFeatureDemoOuterClass.RecFeature> protoClass = RecFeatureDemoOuterClass.RecFeature.class;
        // 确保keyExtractor是可序列化的（显式类或Flink的Function）
        Function<RecFeatureDemoOuterClass.RecFeature, String> keyExtractor = new UserKeyExtractor();


        // 7. 添加Redis Sink（使用自定义的protobuf参数）
        RedisUtil.addRedisSink(
                dataStream,
                redisConfig,
                true, // 异步写入
                100  // 批量大小
                //protoClass,
                //keyExtractor
        );

        // 8. 执行任务
        env.execute("Protobuf to Redis Example");
    }

    //设置 key值
    private static class UserKeyExtractor implements Function<RecFeatureDemoOuterClass.RecFeature, String>, Serializable {
        @Override
        public String apply(RecFeatureDemoOuterClass.RecFeature feature) {
            return PREFIX + feature.getKey();
        }
    }
}
