package com.gosh.job;

import com.gosh.entity.*;
import com.gosh.util.FlinkEnvUtil;
import com.gosh.util.FlinkMonitorUtil;
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
    private static final String JOB_NAME = "Kafka Post Event Processing Job"; // 统一作业名称

    public static void main(String[] args) throws Exception {

        // 第一步：创建flink环境
        StreamExecutionEnvironment env = FlinkEnvUtil.createStreamExecutionEnvironment();

        // 第二步：创建Source，Kafka环境
        KafkaSource<KafkaRawEvent> inputTopic = KafkaEnvUtil.createKafkaRawSource(
                KafkaEnvUtil.loadProperties(), "post"
        );

        // 第三步：使用KafkaSource创建DataStream
        DataStreamSource<KafkaRawEvent> kafkaSource = env.fromSource(
                inputTopic,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Source"
        );

        // 第四步：处理逻辑
        // 创建Jackson ObjectMapper用于JSON处理
        ObjectMapper objectMapper = new ObjectMapper();
        // 解析JSON为PostEvent实体，并筛选出event_type=16的数据
        DataStream<PostEvent> filteredEvents = kafkaSource
                .map(kafkaRawEvent -> {
                    try {
                        PostEvent postEvent = objectMapper.readValue(kafkaRawEvent.getMessage(), PostEvent.class);
                        postEvent.setEvenTime(kafkaRawEvent.getTimestamp());

                        return postEvent;
                    } catch (Exception e) {
                        LOG.info("异常解析数据：{}",kafkaRawEvent);
                        return null;
                    }
                })
                .filter(new FilterFunction<PostEvent>() {
                    @Override
                    public boolean filter(PostEvent event) {
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
                            LOG.info("PostEvent缺少必要的post_expose或list字段");
                            return;
                        }


                        // 遍历list中的每个元素，创建对应的ParsedPostEvent
                        for (PostItem item : event.getPostExpose().getList()) {
                            ParsedPostEvent parsed = new ParsedPostEvent();

                            // 设置event_type
                            parsed.setEventType(event.getEventType());
                            parsed.setEventTime(event.getEvenTime());

                            // 设置post item相关字段
                            parsed.setPostId(item.getPostId());
                            parsed.setExposedPos(item.getExposedPos());
                            parsed.setExpoTime(item.getExpoTime());
                            parsed.setRecToken(item.getRecToken());
                            parsed.setFromIndex(item.getFromIndex() == null?"" : item.getFromIndex());
                            parsed.setDestIndex(item.getDestIndex()== null?"" : item.getDestIndex());

                            // 设置post_expose中的其他字段
                            PostExpose expose = event.getPostExpose();
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
                        LOG.info("最终输出结果：{}"  , jsonString);
                        return jsonString;
                    } catch (Exception e) {
                        LOG.info("对象转JSON失败: {}" , e.getMessage());
                        return null;
                    }
                })
                .filter(jsonString -> jsonString != null); // 过滤掉转换失败的记录

        // 第五步：添加反压监控（在输出到Kafka前监控队列压力）
        //DataStream<String> monitoredStream = outputJson
                //.map(new BackpressureMonitor(JOB_NAME,"filter-operator")) // 引入反压监控器
       //         .name("Monitor");

        KafkaSink<String> kafkaSink = KafkaEnvUtil.createKafkaSink(KafkaEnvUtil.loadProperties(), "post_parse");
        outputJson.sinkTo(kafkaSink);

        // 第六步：使用监控工具类执行作业（替代原生execute方法）
        FlinkMonitorUtil.executeWithMonitor(env,JOB_NAME);

        // 执行Flink作业
        //env.execute("Kafka Post Event Processing Job");
    }
}
