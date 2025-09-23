package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 调整后的解析后Post事件实体类，完全匹配目标JSON格式
 */
public class ParsedPostEvent {
    @JsonProperty("event_type")
    private Integer eventType;

    @JsonProperty("post_id")
    private String postId;

    @JsonProperty("exposed_pos")
    private Integer exposedPos;

    @JsonProperty("expo_time")
    private Long expoTime;

    @JsonProperty("rec_token")
    private String recToken;

    @JsonProperty("created_at")
    private Long createdAt;

    private Long UID;
    private String DID;
    private String APP;
    private String SMID;
    private String Version;
    private String Channel;
    private String Platform;
    private String Brand;
    private String OS;
    private String Model;
    private String Lang;
    private String Country;
    private Integer US;
    private String Seq;
    private String Network;
    private Integer FeSystem;

    @JsonProperty("SubPartnerChannel")
    private String subPartnerChannel;

    @JsonProperty("ClientIP")
    private String clientIP;

    private String ADID;
    private String GAID;
    private String IDFA;

    // Getter和Setter方法
    public Integer getEventType() {
        return eventType;
    }

    public void setEventType(Integer eventType) {
        this.eventType = eventType;
    }

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

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUID() {
        return UID;
    }

    public void setUID(Long UID) {
        this.UID = UID;
    }

    public String getDID() {
        return DID;
    }

    public void setDID(String DID) {
        this.DID = DID;
    }

    public String getAPP() {
        return APP;
    }

    public void setAPP(String APP) {
        this.APP = APP;
    }

    public String getSMID() {
        return SMID;
    }

    public void setSMID(String SMID) {
        this.SMID = SMID;
    }

    public String getVersion() {
        return Version;
    }

    public void setVersion(String version) {
        Version = version;
    }

    public String getChannel() {
        return Channel;
    }

    public void setChannel(String channel) {
        Channel = channel;
    }

    public String getPlatform() {
        return Platform;
    }

    public void setPlatform(String platform) {
        Platform = platform;
    }

    public String getBrand() {
        return Brand;
    }

    public void setBrand(String brand) {
        Brand = brand;
    }

    public String getOS() {
        return OS;
    }

    public void setOS(String OS) {
        this.OS = OS;
    }

    public String getModel() {
        return Model;
    }

    public void setModel(String model) {
        Model = model;
    }

    public String getLang() {
        return Lang;
    }

    public void setLang(String lang) {
        Lang = lang;
    }

    public String getCountry() {
        return Country;
    }

    public void setCountry(String country) {
        Country = country;
    }

    public Integer getUS() {
        return US;
    }

    public void setUS(Integer US) {
        this.US = US;
    }

    public String getSeq() {
        return Seq;
    }

    public void setSeq(String seq) {
        Seq = seq;
    }

    public String getNetwork() {
        return Network;
    }

    public void setNetwork(String network) {
        Network = network;
    }

    public Integer getFeSystem() {
        return FeSystem;
    }

    public void setFeSystem(Integer feSystem) {
        FeSystem = feSystem;
    }

    public String getSubPartnerChannel() {
        return subPartnerChannel;
    }

    public void setSubPartnerChannel(String subPartnerChannel) {
        this.subPartnerChannel = subPartnerChannel;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getADID() {
        return ADID;
    }

    public void setADID(String ADID) {
        this.ADID = ADID;
    }

    public String getGAID() {
        return GAID;
    }

    public void setGAID(String GAID) {
        this.GAID = GAID;
    }

    public String getIDFA() {
        return IDFA;
    }

    public void setIDFA(String IDFA) {
        this.IDFA = IDFA;
    }
}
