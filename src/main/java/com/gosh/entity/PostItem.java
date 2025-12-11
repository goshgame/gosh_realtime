package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * list数组中的单个元素实体类
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PostItem {
    @JsonProperty("post_id")
    private String postId;

    @JsonProperty("exposed_pos")
    private Integer exposedPos;

    @JsonProperty("expo_time")
    private Long expoTime;

    @JsonProperty("rec_token")
    private String recToken;

    @JsonProperty("from_index")
    private String fromIndex;

    @JsonProperty("dest_index")
    private String destIndex;

    // Getter和Setter方法
    public String getPostId() {
        return postId;
    }

    public void setPostId(String postId) {
        this.postId = postId;
    }

    public Integer getExposedPos() {
        return exposedPos;
    }

    public void setExposedPos(Integer exposedPos) {
        this.exposedPos = exposedPos;
    }

    public Long getExpoTime() {
        return expoTime;
    }

    public void setExpoTime(Long expoTime) {
        this.expoTime = expoTime;
    }

    public String getRecToken() {
        return recToken;
    }

    public void setRecToken(String recToken) {
        this.recToken = recToken;
    }

    public String getFromIndex() {
        return fromIndex;
    }

    public void setFromIndex(String fromIndex) {
        this.fromIndex = fromIndex;
    }

    public String getDestIndex() {
        return destIndex;
    }

    public void setDestIndex(String destIndex) {
        this.destIndex = destIndex;
    }

    @Override
    public String toString() {
        return "PostItem{" +
                "postId='" + postId + '\'' +
                ", exposedPos=" + exposedPos +
                ", expoTime=" + expoTime +
                ", recToken='" + recToken + '\'' +
                ", fromIndex='" + fromIndex + '\'' +
                ", destIndex='" + destIndex + '\'' +
                '}';
    }
}