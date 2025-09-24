package com.gosh.job;

import com.gosh.entity.PostEvent;
import com.gosh.entity.ParsedPostEvent;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.KafkaEnvUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PostPoseParseJob {
    private static final Logger LOG = LoggerFactory.getLogger(PostPoseParseJob.class);
    public static void main(String[] args) throws Exception {

        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

        // 第二步：创建Source，Kafka环境
        KafkaSource<String> inputTopic = KafkaEnvUtil.createKafkaSource(
                KafkaEnvUtil.loadProperties(), "post"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<String> kafkaSource = env.fromSource(
                inputTopic,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source"
        );

        // 创建Jackson ObjectMapper用于JSON处理
        ObjectMapper objectMapper = new ObjectMapper();
        // 解析JSON为PostEvent实体，并筛选出event_type=16的数据
        DataStream<PostEvent> filteredEvents = kafkaSource
                .map(jsonString -> {
                    try {
                        return objectMapper.readValue(jsonString, PostEvent.class);
                    } catch (Exception e) {
                        LOG.info("异常解析数据：{}",jsonString);
                        return null;
                    }
                })
                .filter(new FilterFunction<PostEvent>() {
                    @Override
                    public boolean filter(PostEvent event) {
                        // 过滤掉解析失败的记录和event_type不等于16的记录
                        return event != null && event.getEventType() != null && event.getEventType() == 16;
                    }
                });

        // 将嵌套结构展平为ParsedPostEvent，并将每条记录拆分为多条
        DataStream<ParsedPostEvent> parsedEvents = filteredEvents
                .flatMap(new FlatMapFunction<PostEvent, ParsedPostEvent>() {
                    @Override
                    public void flatMap(PostEvent event, Collector<ParsedPostEvent> collector) {
                        // 检查必要字段是否存在
                        if (event.getPostExpose() == null || event.getPostExpose().getList() == null) {
                            System.err.println("PostEvent缺少必要的post_expose或list字段");
                            return;
                        }


                        // 遍历list中的每个元素，创建对应的ParsedPostEvent
                        for (PostEvent.PostItem item : event.getPostExpose().getList()) {
                            ParsedPostEvent parsed = new ParsedPostEvent();

                            // 设置event_type
                            parsed.setEventType(event.getEventType());

                            // 设置post item相关字段
                            parsed.setPostId(item.getPostId());
                            parsed.setExposedPos(item.getExposedPos());
                            parsed.setExpoTime(item.getExpoTime());
                            parsed.setRecToken(item.getRecToken());

                            // 设置post_expose中的其他字段
                            PostEvent.PostExpose expose = event.getPostExpose();
                            parsed.setCreatedAt(expose.getCreatedAt());
                            parsed.setUID(expose.getUid());
                            parsed.setDID(expose.getDID());
                            parsed.setAPP(expose.getAPP());
                            parsed.setSMID(expose.getSMID());
                            parsed.setVersion(expose.getVersion());
                            parsed.setChannel(expose.getChannel());
                            parsed.setPlatform(expose.getPlatform());
                            parsed.setBrand(expose.getBrand());
                            parsed.setOS(expose.getOS());
                            parsed.setModel(expose.getModel());
                            parsed.setLang(expose.getLang());
                            parsed.setCountry(expose.getCountry());
                            parsed.setUS(expose.getUS());
                            parsed.setSeq(expose.getSeq());
                            parsed.setNetwork(expose.getNetwork());
                            parsed.setFeSystem(expose.getFeSystem());
                            parsed.setSubPartnerChannel(expose.getSubPartnerChannel());
                            parsed.setClientIP(expose.getClientIP());
                            parsed.setADID(expose.getADID());
                            parsed.setGAID(expose.getGAID());
                            parsed.setIDFA(expose.getIDFA());
                            collector.collect(parsed);
                        }
                    }
                });

        // 将ParsedPostEvent转换为JSON字符串
        DataStream<String> outputJson = parsedEvents
                .map(parsedEvent -> {
                    try {
                        String jsonString = objectMapper.writeValueAsString(parsedEvent);
                        System.out.println("最终输出结果：" + jsonString);
                        return jsonString;
                    } catch (Exception e) {
                        System.err.println("对象转JSON失败: " + e.getMessage());
                        return null;
                    }
                })
                .filter(jsonString -> jsonString != null); // 过滤掉转换失败的记录


        KafkaSink<String> kafkaSink = KafkaEnvUtil.createKafkaSink(KafkaEnvUtil.loadProperties(), "post_parse");
        outputJson.sinkTo(kafkaSink);

        // 执行Flink作业
        env.execute("Kafka Post Event Processing Job");
    }
}
