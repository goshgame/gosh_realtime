package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * 调整后的解析后Post事件实体类，完全匹配目标JSON格式
 */
/**
 * 调整后的解析后Post事件实体类，完全匹配目标JSON格式
 */
@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.ANY,  // 识别所有字段
        getterVisibility = JsonAutoDetect.Visibility.NONE  // 忽略getter方法生成的键
)
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

    @JsonProperty("from_index")
    private String fromIndex;

    @JsonProperty("dest_index")
    private String destIndex;

    @JsonProperty("created_at")
    private Long createdAt;

    @JsonProperty("UID")
    private Long UID;
    @JsonProperty("DID")
    private String DID;
    @JsonProperty("APP")
    private String APP;
    @JsonProperty("SMID")
    private String SMID;
    @JsonProperty("Version")
    private String Version;
    @JsonProperty("Channel")
    private String Channel;
    @JsonProperty("Platform")
    private String Platform;
    @JsonProperty("Brand")
    private String Brand;
    @JsonProperty("OS")
    private String OS;
    @JsonProperty("Model")
    private String Model;
    @JsonProperty("Lang")
    private String Lang;
    @JsonProperty("Country")
    private String Country;
    @JsonProperty("US")
    private Integer US;
    @JsonProperty("Seq")
    private String Seq;
    @JsonProperty("Network")
    private String Network;
    @JsonProperty("FeSystem")
    private Integer FeSystem;

    @JsonProperty("SubPartnerChannel")
    private String subPartnerChannel;

    @JsonProperty("ClientIP")
    private String clientIP;

    @JsonProperty("ADID")
    private String ADID;
    @JsonProperty("GAID")
    private String GAID;
    @JsonProperty("IDFA")
    private String IDFA;
    @JsonProperty("event_time")
    private Long eventTime;

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

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "ParsedPostEvent{" +
                "eventType=" + eventType +
                ", postId='" + postId + '\'' +
                ", exposedPos=" + exposedPos +
                ", expoTime=" + expoTime +
                ", recToken='" + recToken + '\'' +
                ", fromIndex='" + fromIndex + '\'' +
                ", destIndex='" + destIndex + '\'' +
                ", createdAt=" + createdAt +
                ", UID=" + UID +
                ", DID='" + DID + '\'' +
                ", APP='" + APP + '\'' +
                ", SMID='" + SMID + '\'' +
                ", Version='" + Version + '\'' +
                ", Channel='" + Channel + '\'' +
                ", Platform='" + Platform + '\'' +
                ", Brand='" + Brand + '\'' +
                ", OS='" + OS + '\'' +
                ", Model='" + Model + '\'' +
                ", Lang='" + Lang + '\'' +
                ", Country='" + Country + '\'' +
                ", US=" + US +
                ", Seq='" + Seq + '\'' +
                ", Network='" + Network + '\'' +
                ", FeSystem=" + FeSystem +
                ", subPartnerChannel='" + subPartnerChannel + '\'' +
                ", clientIP='" + clientIP + '\'' +
                ", ADID='" + ADID + '\'' +
                ", GAID='" + GAID + '\'' +
                ", IDFA='" + IDFA + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
