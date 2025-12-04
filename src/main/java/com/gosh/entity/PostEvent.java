package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 顶层事件实体类，对应完整的JSON数据结构
 * {
 *     "event_type": 16,
 *     "post_expose": {
 *         "uid": 114704,
 *         "list": [
 *             {
 *                 "post_id": "270312310000018891",
 *                 "exposed_pos": 2,
 *                 "expo_time": 1757987353,
 *                 "rec_token": "b6e4df34-1e87-41af-84ec-37938b52b636"
 *             }
 *         ],
 *         "created_at": 1757987352,
 *         "UID": 114704,
 *         "DID": "e3f5d522db887f7f",
 *         "APP": "vizz",
 *         "SMID": "20250523151706cfb5fc4eb912169d914139fe0f6ee2be01e6f5692c27698b",
 *         "Version": "3.4.0",
 *         "Channel": "fbsn",
 *         "Platform": "android",
 *         "Brand": "vivo",
 *         "OS": "Android 13",
 *         "Model": "V2188A",
 *         "Lang": "en",
 *         "Country": "zh",
 *         "US": 0,
 *         "Seq": "",
 *         "Network": "wifi",
 *         "FeSystem": 0,
 *         "SubPartnerChannel": "",
 *         "ClientIP": "98.98.232.216",
 *         "ADID": "6f6b93dafc54da2c8c7e54232cd6257d",
 *         "GAID": "66e647f1-da29-479b-8d41-d4d527e7f853",
 *         "IDFA": ""
 *     }
 * }
 *
 * --  变更后数据
 * {
 *     "event_type": 16,
 *     "post_expose": {
 *         "uid": 12986951,
 *         "list": [
 *             {
 *                 "post_id": "289037330000013196",
 *                 "exposed_pos": 2,
 *                 "expo_time": 1760602650,
 *                 "rec_token": "bf462526-f917-446d-aa0b-8e44b013952c"
 *             }
 *         ],
 *         "created_at": 1760602651,
 *         "did": "ce516ac4e9e86f27",
 *         "app": "hotya",
 *         "smid": "20250928194851347b00641999c7996a61bda45dfb634d01c1f053aa3c6155",
 *         "version": "3.4.3",
 *         "channel": "hotya",
 *         "platform": "android",
 *         "brand": "motorola",
 *         "os": "Android 14",
 *         "model": "moto g85 5G",
 *         "lang": "hi-Latn",
 *         "country": "in",
 *         "network": "mobile",
 *         "client_ip": "223.228.63.189",
 *         "adid": "2a05c7b67f5799b3296ebc41aa6ee3ec",
 *         "gaid": "cd17752a-0b41-4293-8c3e-c05dfae42131"
 *     }
 * }
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonIgnoreProperties(ignoreUnknown = true)
public class PostEvent {
    @JsonProperty("event_type")
    private Integer eventType;

    @JsonProperty("post_expose")
    private PostExpose postExpose;

    //最原始的进入kafka的时间，元数据中的timestamp（TIMESTAMP_LTZ(3) 单位ms
    @JsonProperty("even_time")
    private Long evenTime;

    public PostExpose getPostExpose() {
        return postExpose;
    }

    public void setPostExpose(PostExpose postExpose) {
        this.postExpose = postExpose;
    }

    public Integer getEventType() {
        return eventType;
    }

    public void setEventType(Integer eventType) {
        this.eventType = eventType;
    }

    public Long getEvenTime() {
        return evenTime;
    }

    public void setEvenTime(Long evenTime) {
        this.evenTime = evenTime;
    }

    @Override
    public String toString() {
        return "PostEvent{" +
                "eventType=" + eventType +
                ", postExpose=" + postExpose +
                ", evenTime=" + evenTime +
                '}';
    }
}
