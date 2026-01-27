package com.gosh.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP Client 工具类
 * 基于 java.net.http.HttpClient 实现
 */
public class HttpUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

    // 全局共享 HttpClient，复用连接池
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();

    /**
     * 发送 GET 请求
     *
     * @param url 请求地址
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String get(String url) {
        return get(url, Duration.ofSeconds(10));
    }

    /**
     * 发送 GET 请求（带超时）
     *
     * @param url     请求地址
     * @param timeout 超时时间
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String get(String url, Duration timeout) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(timeout)
                    .GET()
                    .build();

            HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            } else {
                LOG.warn("HTTP GET failed. URL: {}, Status: {}, Body: {}", url, response.statusCode(), response.body());
            }
        } catch (Exception e) {
            LOG.error("HTTP GET error. URL: {}", url, e);
        }
        return null;
    }

    /**
     * 发送 POST 请求 (Content-Type: application/json)
     *
     * @param url      请求地址
     * @param jsonBody JSON 请求体
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String post(String url, String jsonBody) {
        return post(url, jsonBody, Duration.ofSeconds(10));
    }

    /**
     * 发送 POST 请求 (Content-Type: application/json)（带超时）
     *
     * @param url      请求地址
     * @param jsonBody JSON 请求体
     * @param timeout  超时时间
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String post(String url, String jsonBody, Duration timeout) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .timeout(timeout)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            } else {
                LOG.warn("HTTP POST failed. URL: {}, Status: {}, Body: {}", url, response.statusCode(),
                        response.body());
            }
        } catch (Exception e) {
            LOG.error("HTTP POST error. URL: {}", url, e);
        }
        return null;
    }

    /**
     * 发送 POST 请求 (带 Header)
     *
     * @param url      请求地址
     * @param headers  请求头
     * @param jsonBody JSON 请求体
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String post(String url, Map<String, String> headers, String jsonBody) {
        return post(url, headers, jsonBody, Duration.ofSeconds(10));
    }

    /**
     * 发送 POST 请求 (带 Header 和超时)
     *
     * @param url      请求地址
     * @param headers  请求头
     * @param jsonBody JSON 请求体
     * @param timeout  超时时间
     * @return 响应内容字符串，请求失败返回 null
     */
    public static String post(String url, Map<String, String> headers, String jsonBody, Duration timeout) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(timeout);

            if (headers != null) {
                headers.forEach(builder::header);
            }

            if (headers == null || headers.keySet().stream().noneMatch(k -> "Content-Type".equalsIgnoreCase(k))) {
                builder.header("Content-Type", "application/json");
            }

            HttpRequest request = builder
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            } else {
                LOG.warn("HTTP POST failed. URL: {}, Status: {}, Body: {}", url, response.statusCode(),
                        response.body());
            }
        } catch (Exception e) {
            LOG.error("HTTP POST error. URL: {}", url, e);
        }
        return null;
    }
}