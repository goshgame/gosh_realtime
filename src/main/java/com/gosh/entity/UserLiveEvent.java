package com.gosh.entity;

public class UserLiveEvent {
    private String uid;
    private String liveId;
    private long watchTime;
    private String rawEventTime;
    private String eventTime;

    public UserLiveEvent() {
    }

    public UserLiveEvent(String uid, String liveId, long watchTime, String rawEventTime, String eventTime) {
        this.uid = uid;
        this.liveId = liveId;
        this.watchTime = watchTime;
        this.rawEventTime = rawEventTime;
        this.eventTime = eventTime;
    }

    // Getter 和 Setter 方法
    public String getUid() { return uid; }
    public void setUid(String uid) { this.uid = uid; }
    public String getLiveId() { return liveId; }
    public void setLiveId(String liveId) { this.liveId = liveId; }
    public long getWatchTime() { return watchTime; }
    public void setWatchTime(long watchTime) { this.watchTime = watchTime; }
    public String getRawEventTime() { return rawEventTime; }
    public void setRawEventTime(String rawEventTime) { this.rawEventTime = rawEventTime; }
    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }

    @Override
    public String toString() {
        return "UserLiveEvent{" +
                "uid='" + uid + '\'' +
                ", liveId='" + liveId + '\'' +
                ", watchTime=" + watchTime +
                ", rawEventTime='" + rawEventTime + '\'' +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
