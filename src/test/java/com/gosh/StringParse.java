package com.gosh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.entity.*;

import java.time.Instant;
import java.util.Date;

public class StringParse {
    public static void main(String[] args) throws JsonProcessingException {
        String line ="{\"event_type\":16,\"post_expose\":{\"uid\":114704,\"list\":[{\"post_id\":\"270312310000018891\",\"exposed_pos\":2,\"expo_time\":1757987353,\"rec_token\":\"b6e4df34-1e87-41af-84ec-37938b52b636\"}],\"created_at\":1757987352,\"UID\":114704,\"DID\":\"e3f5d522db887f7f\",\"APP\":\"vizz\",\"SMID\":\"20250523151706cfb5fc4eb912169d914139fe0f6ee2be01e6f5692c27698b\",\"Version\":\"3.4.0\",\"Channel\":\"fbsn\",\"Platform\":\"android\",\"Brand\":\"vivo\",\"OS\":\"Android 13\",\"Model\":\"V2188A\",\"Lang\":\"en\",\"Country\":\"zh\",\"US\":0,\"Seq\":\"\",\"Network\":\"wifi\",\"FeSystem\":0,\"SubPartnerChannel\":\"\",\"ClientIP\":\"98.98.232.216\",\"ADID\":\"6f6b93dafc54da2c8c7e54232cd6257d\",\"GAID\":\"66e647f1-da29-479b-8d41-d4d527e7f853\",\"IDFA\":\"\"}}\n";
        String line2 ="{\"event_type\":16,\"post_expose\":{\"uid\":12255919,\"list\":[{\"post_id\":\"285166820000015329\",\"exposed_pos\":2,\"expo_time\":1758854509,\"rec_token\":\"sim_bdc4711b-020c-4bba-a553-11aa7cece5f8\",\"from_index\":\"10.1\",\"dest_index\":\"11.2\"}],\"created_at\":1758854508,\"UID\":12255919,\"DID\":\"3753f73f0b6b0672\",\"APP\":\"hotya\",\"SMID\":\"20250916104724e83a8f88d7a6cb44c9f3316f049830170162bd5152fcccbe\",\"Version\":\"3.4.0\",\"Channel\":\"google\",\"Platform\":\"android\",\"Brand\":\"motorola\",\"OS\":\"Android 15\",\"Model\":\"moto g64 5G\",\"Lang\":\"en\",\"Country\":\"en\",\"US\":0,\"Seq\":\"\",\"Network\":\"wifi\",\"FeSystem\":0,\"SubPartnerChannel\":\"\",\"ClientIP\":\"2405:201:5c05:a8d7:c4fb:84f:17eb:1b95\",\"ADID\":\"2f0d72238b275466cc92a423834b6521\",\"GAID\":\"00000000-0000-0000-0000-000000000000\",\"IDFA\":\"\"}}\n";
        String line3 ="{\"event_type\":16,\"post_expose\":{\"uid\":12575030,\"list\":[{\"post_id\":\"301045950000129770\",\"exposed_pos\":2,\"expo_time\":1758837127,\"rec_token\":\"sim_7fa967d0-5fd8-4084-aa16-4b69c4a44c54\",\"from_index\":\"7.0\",\"dest_index\":\"7.1\"}],\"created_at\":1758837128,\"UID\":12575030,\"DID\":\"bde1f32a9474c87a\",\"APP\":\"hotya\",\"SMID\":\"2025092306530547478c5e66a0164ee723c26c75aa76ac01ae774695f495ad\",\"Version\":\"3.4.1\",\"Channel\":\"hotya\",\"Platform\":\"android\",\"Brand\":\"vivo\",\"OS\":\"Android 12\",\"Model\":\"V2204\",\"Lang\":\"en\",\"Country\":\"us\",\"US\":0,\"Seq\":\"\",\"Network\":\"mobile\",\"FeSystem\":0,\"SubPartnerChannel\":\"\",\"ClientIP\":\"152.57.167.166\",\"ADID\":\"c93f158314e5c3dcac241c1a7d7dba49\",\"GAID\":\"0caffbf8-216a-4338-8fb6-f7581035189a\",\"IDFA\":\"\"}}\n" ;

        KafkaRawEvent testKafkaRawEvent = createTestKafkaRawEvent();

        ObjectMapper objectMapper = new ObjectMapper();

        //转PostEvent
        String message = testKafkaRawEvent.getMessage();
        long timestamp = testKafkaRawEvent.getTimestamp();



        PostEvent postEvent = objectMapper.readValue(message, PostEvent.class);
        postEvent.setEvenTime(timestamp);
        System.out.println(postEvent);



        for (PostItem item : postEvent.getPostExpose().getList()) {
            ParsedPostEvent parsed = new ParsedPostEvent();

            // 设置event_type
            parsed.setEventType(postEvent.getEventType());

            // 设置post item相关字段
            parsed.setPostId(item.getPostId());
            parsed.setExposedPos(item.getExposedPos());
            parsed.setExpoTime(item.getExpoTime());
            parsed.setRecToken(item.getRecToken());
            parsed.setFromIndex(item.getFromIndex() == null?"" : item.getFromIndex());
            parsed.setDestIndex(item.getDestIndex()== null?"" : item.getDestIndex());

            // 设置post_expose中的其他字段
            PostExpose expose = postEvent.getPostExpose();
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
            parsed.setEventTime(postEvent.getEvenTime());

            String jsonString = objectMapper.writeValueAsString(parsed);
            System.out.println(jsonString);
        }

    }

    // 生成测试用KafkaRawEvent对象
    public static KafkaRawEvent createTestKafkaRawEvent() {
        KafkaRawEvent testEvent = new KafkaRawEvent();

        // 1. 设置message字段：使用用户指定的PostEvent JSON内容
        String testMessage = "{" +
                "\"event_type\":16," +
                "\"post_expose\":{" +
                "\"uid\":12575030," +
                "\"list\":[{" +
                "\"post_id\":\"301045950000129770\"," +
                "\"exposed_pos\":2," +
                "\"expo_time\":1758837127," +
                "\"rec_token\":\"sim_7fa967d0-5fd8-4084-aa16-4b69c4a44c54\"," +
                "\"from_index\":\"7.0\"," +
                "\"dest_index\":\"7.1\"" +
                "}]," +
                "\"created_at\":1758837128," +
                "\"UID\":12575030," +
                "\"DID\":\"bde1f32a9474c87a\"," +
                "\"APP\":\"hotya\"," +
                "\"SMID\":\"2025092306530547478c5e66a0164ee723c26c75aa76ac01ae774695f495ad\"," +
                "\"Version\":\"3.4.1\"," +
                "\"Channel\":\"hotya\"," +
                "\"Platform\":\"android\"," +
                "\"Brand\":\"vivo\"," +
                "\"OS\":\"Android 12\"," +
                "\"Model\":\"V2204\"," +
                "\"Lang\":\"en\"," +
                "\"Country\":\"us\"," +
                "\"US\":0," +
                "\"Seq\":\"\"," +
                "\"Network\":\"mobile\"," +
                "\"FeSystem\":0," +
                "\"SubPartnerChannel\":\"\"," +
                "\"ClientIP\":\"152.57.167.166\"," +
                "\"ADID\":\"c93f158314e5c3dcac241c1a7d7dba49\"," +
                "\"GAID\":\"0caffbf8-216a-4338-8fb6-f7581035189a\"," +
                "\"IDFA\":\"\"" +
                "}" +
                "}";
        testEvent.setMessage(testMessage);

        // 2. 设置timestamp字段：与post_expose.created_at对齐（转毫秒级，方便后续赋值给evenTime）
        // 注：created_at是秒级时间戳（1758837128），转毫秒需×1000
        long testTimestamp = 1759040374381L;
        testEvent.setTimestamp(testTimestamp);

        // 3. 设置offset和partition（模拟Kafka消息元数据，可自定义）
        testEvent.setOffset(1000L);       // 模拟消息在分区中的偏移量
        testEvent.setPartition(1);        // 模拟消息所在的Kafka分区

        return testEvent;
    }

}

