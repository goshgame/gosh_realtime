package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * post_expose字段对应的实体类
 */
public class PostExpose {
    @JsonProperty("uid")
    private Long uid;

    @JsonProperty("UID")
    private Long UUID;

    @JsonProperty("list")
    private List<PostItem> list;

    @JsonProperty("created_at")
    private Long createdAt;

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

    // Getter和Setter方法
    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Long getUUID() {
        return UUID;
    }

    public void setUUID(Long UUID) {
        this.UUID = UUID;
    }

    public List<PostItem> getList() {
        return list;
    }

    public void setList(List<PostItem> list) {
        this.list = list;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
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

    @Override
    public String toString() {
        return "PostExpose{" +
                "uid=" + uid +
                ", UUID=" + UUID +
                ", list=" + list +
                ", createdAt=" + createdAt +
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
                '}';
    }
}