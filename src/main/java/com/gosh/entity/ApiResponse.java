package com.gosh.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 通用 API 响应结构体
 * 支持泛型数据体，灵活适配不同接口返回格式
 * 
 * @param <T> 数据体类型
 */
public class ApiResponse<T> {
    @JsonProperty("code")
    private Integer code;

    @JsonProperty("data")
    private T data;

    @JsonProperty("server_time")
    private Long serverTime;

    public ApiResponse() {
    }

    public ApiResponse(Integer code, T data, Long serverTime) {
        this.code = code;
        this.data = data;
        this.serverTime = serverTime;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Long getServerTime() {
        return serverTime;
    }

    public void setServerTime(Long serverTime) {
        this.serverTime = serverTime;
    }

    /**
     * 判断 API 调用是否成功
     * code == 0 表示成功
     */
    public boolean isSuccess() {
        return code != null && code == 0;
    }

    @Override
    public String toString() {
        return "ApiResponse{" +
                "code=" + code +
                ", data=" + data +
                ", serverTime=" + serverTime +
                '}';
    }
}
