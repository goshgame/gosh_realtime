package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;
import java.util.Map;

public class FeastRequest {
    @JsonProperty("project")
    private String project;

    @JsonProperty("feature_view_name")
    private String featureViewName;

    @JsonProperty("data")
    private List<FeastData> data;

    @JsonProperty("ttl")
    private Integer ttl;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getFeatureViewName() {
        return featureViewName;
    }

    public void setFeatureViewName(String featureViewName) {
        this.featureViewName = featureViewName;
    }

    public List<FeastData> getData() {
        return data;
    }

    public void setData(List<FeastData> data) {
        this.data = data;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    public static class FeastData {
        @JsonProperty("entity_key")
        private EntityKey entityKey;

        @JsonProperty("features")
        private Map<String, FeatureValue> features;

        @JsonProperty("event_timestamp")
        private Long eventTimestamp;

        public EntityKey getEntityKey() {
            return entityKey;
        }

        public void setEntityKey(EntityKey entityKey) {
            this.entityKey = entityKey;
        }

        public Map<String, FeatureValue> getFeatures() {
            return features;
        }

        public void setFeatures(Map<String, FeatureValue> features) {
            this.features = features;
        }

        public Long getEventTimestamp() {
            return eventTimestamp;
        }

        public void setEventTimestamp(Long eventTimestamp) {
            this.eventTimestamp = eventTimestamp;
        }
    }

    public static class EntityKey {
        @JsonProperty("join_keys")
        private List<String> joinKeys;

        @JsonProperty("entity_values")
        private List<Object> entityValues;

        public List<String> getJoinKeys() {
            return joinKeys;
        }

        public void setJoinKeys(List<String> joinKeys) {
            this.joinKeys = joinKeys;
        }

        public List<Object> getEntityValues() {
            return entityValues;
        }

        public void setEntityValues(List<Object> entityValues) {
            this.entityValues = entityValues;
        }
    }

    public static class FeatureValue {
        @JsonProperty("value_type")
        private ValueType valueType;

        @JsonProperty("int64_val")
        private Long int64Val;

        @JsonProperty("uint64_val")
        private Long uint64Val;

        @JsonProperty("int32_val")
        private Integer int32Val;

        @JsonProperty("uint32_val")
        private Long uint32Val;

        @JsonProperty("float32_val")
        private Float float32Val;

        @JsonProperty("float64_val")
        private Double float64Val;

        @JsonProperty("bool_val")
        private Boolean boolVal;

        @JsonProperty("string_val")
        private String stringVal;

        @JsonProperty("bytes_val")
        private byte[] bytesVal;

        @JsonProperty("int64_slice_val")
        private List<Long> int64SliceVal;

        @JsonProperty("uint64_slice_val")
        private List<Long> uint64SliceVal;

        @JsonProperty("int32_slice_val")
        private List<Integer> int32SliceVal;

        @JsonProperty("uint32_slice_val")
        private List<Long> uint32SliceVal;

        @JsonProperty("float32_slice_val")
        private List<Float> float32SliceVal;

        @JsonProperty("float64_slice_val")
        private List<Double> float64SliceVal;

        @JsonProperty("bool_slice_val")
        private List<Boolean> boolSliceVal;

        @JsonProperty("string_slice_val")
        private List<String> stringSliceVal;

        public ValueType getValueType() {
            return valueType;
        }

        public void setValueType(ValueType valueType) {
            this.valueType = valueType;
        }

        public Long getInt64Val() {
            return int64Val;
        }

        public void setInt64Val(Long int64Val) {
            this.int64Val = int64Val;
        }

        public Long getUint64Val() {
            return uint64Val;
        }

        public void setUint64Val(Long uint64Val) {
            this.uint64Val = uint64Val;
        }

        public Integer getInt32Val() {
            return int32Val;
        }

        public void setInt32Val(Integer int32Val) {
            this.int32Val = int32Val;
        }

        public Long getUint32Val() {
            return uint32Val;
        }

        public void setUint32Val(Long uint32Val) {
            this.uint32Val = uint32Val;
        }

        public Float getFloat32Val() {
            return float32Val;
        }

        public void setFloat32Val(Float float32Val) {
            this.float32Val = float32Val;
        }

        public Double getFloat64Val() {
            return float64Val;
        }

        public void setFloat64Val(Double float64Val) {
            this.float64Val = float64Val;
        }

        public Boolean getBoolVal() {
            return boolVal;
        }

        public void setBoolVal(Boolean boolVal) {
            this.boolVal = boolVal;
        }

        public String getStringVal() {
            return stringVal;
        }

        public void setStringVal(String stringVal) {
            this.stringVal = stringVal;
        }

        public byte[] getBytesVal() {
            return bytesVal;
        }

        public void setBytesVal(byte[] bytesVal) {
            this.bytesVal = bytesVal;
        }

        public List<Long> getInt64SliceVal() {
            return int64SliceVal;
        }

        public void setInt64SliceVal(List<Long> int64SliceVal) {
            this.int64SliceVal = int64SliceVal;
        }

        public List<Long> getUint64SliceVal() {
            return uint64SliceVal;
        }

        public void setUint64SliceVal(List<Long> uint64SliceVal) {
            this.uint64SliceVal = uint64SliceVal;
        }

        public List<Integer> getInt32SliceVal() {
            return int32SliceVal;
        }

        public void setInt32SliceVal(List<Integer> int32SliceVal) {
            this.int32SliceVal = int32SliceVal;
        }

        public List<Long> getUint32SliceVal() {
            return uint32SliceVal;
        }

        public void setUint32SliceVal(List<Long> uint32SliceVal) {
            this.uint32SliceVal = uint32SliceVal;
        }

        public List<Float> getFloat32SliceVal() {
            return float32SliceVal;
        }

        public void setFloat32SliceVal(List<Float> float32SliceVal) {
            this.float32SliceVal = float32SliceVal;
        }

        public List<Double> getFloat64SliceVal() {
            return float64SliceVal;
        }

        public void setFloat64SliceVal(List<Double> float64SliceVal) {
            this.float64SliceVal = float64SliceVal;
        }

        public List<Boolean> getBoolSliceVal() {
            return boolSliceVal;
        }

        public void setBoolSliceVal(List<Boolean> boolSliceVal) {
            this.boolSliceVal = boolSliceVal;
        }

        public List<String> getStringSliceVal() {
            return stringSliceVal;
        }

        public void setStringSliceVal(List<String> stringSliceVal) {
            this.stringSliceVal = stringSliceVal;
        }
    }

    public enum ValueType {
        INT64("int64"),
        INT32("int32"),
        UINT64("uint64"),
        UINT32("uint32"),
        FLOAT32("float32"),
        FLOAT64("float64"),
        STRING("string"),
        BYTES("bytes"),
        BOOL("bool"),
        INT64_SLICE("int64_slice"),
        INT32_SLICE("int32_slice"),
        UINT64_SLICE("uint64_slice"),
        UINT32_SLICE("uint32_slice"),
        FLOAT32_SLICE("float32_slice"),
        FLOAT64_SLICE("float64_slice"),
        STRING_SLICE("string_slice"),
        BOOL_SLICE("bool_slice");

        private final String value;

        ValueType(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }
    }
}