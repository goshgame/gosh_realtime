package com.gosh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.entity.ParsedPostEvent;
import com.gosh.entity.PostEvent;

public class StringParse {
    public static void main(String[] args) throws JsonProcessingException {
        String line ="{\"event_type\":16,\"post_expose\":{\"uid\":114704,\"list\":[{\"post_id\":\"270312310000018891\",\"exposed_pos\":2,\"expo_time\":1757987353,\"rec_token\":\"b6e4df34-1e87-41af-84ec-37938b52b636\"}],\"created_at\":1757987352,\"UID\":114704,\"DID\":\"e3f5d522db887f7f\",\"APP\":\"vizz\",\"SMID\":\"20250523151706cfb5fc4eb912169d914139fe0f6ee2be01e6f5692c27698b\",\"Version\":\"3.4.0\",\"Channel\":\"fbsn\",\"Platform\":\"android\",\"Brand\":\"vivo\",\"OS\":\"Android 13\",\"Model\":\"V2188A\",\"Lang\":\"en\",\"Country\":\"zh\",\"US\":0,\"Seq\":\"\",\"Network\":\"wifi\",\"FeSystem\":0,\"SubPartnerChannel\":\"\",\"ClientIP\":\"98.98.232.216\",\"ADID\":\"6f6b93dafc54da2c8c7e54232cd6257d\",\"GAID\":\"66e647f1-da29-479b-8d41-d4d527e7f853\",\"IDFA\":\"\"}}\n";
        ObjectMapper objectMapper = new ObjectMapper();
        PostEvent postEvent = objectMapper.readValue(line, PostEvent.class);
        System.out.println(postEvent);



        for (PostEvent.PostItem item : postEvent.getPostExpose().getList()) {
            ParsedPostEvent parsed = new ParsedPostEvent();

            // 设置event_type
            parsed.setEventType(postEvent.getEventType());

            // 设置post item相关字段
            parsed.setPostId(item.getPostId());
            parsed.setExposedPos(item.getExposedPos());
            parsed.setExpoTime(item.getExpoTime());
            parsed.setRecToken(item.getRecToken());

            // 设置post_expose中的其他字段
            PostEvent.PostExpose expose = postEvent.getPostExpose();
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


            String jsonString = objectMapper.writeValueAsString(parsed);
            System.out.println(jsonString);
        }

    }


}

