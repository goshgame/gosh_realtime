package com.gosh.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.entity.FeastRequest;
import com.gosh.util.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FeastApi {
    private static final Logger LOG = LoggerFactory.getLogger(FeastApi.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // 简单的内存缓存：uid -> token
    private static final Map<Integer, String> TOKEN_CACHE = new ConcurrentHashMap<>();
    // test host
    private static final String TEST_HOST = "https://test-api.gosh0.com";
    // live host
    private static final String LIVE_HOST = "https://api.gosh.com";
    // host
    private static final String HOST = TEST_HOST;
    // login uid
    private static final int LOGIN_UID = 1;

    /**
     * 获取用户Token（带缓存）
     */
    public static String getToken(int uid) {
        if (TOKEN_CACHE.containsKey(uid)) {
            return TOKEN_CACHE.get(uid);
        }

        String jsonResponse = mockUserLogin(uid);
        if (jsonResponse != null) {
            try {
                JsonNode root = OBJECT_MAPPER.readTree(jsonResponse);
                if (root.has("code") && root.get("code").asInt() == 0) {
                    JsonNode data = root.get("data");
                    if (data != null && data.has("token")) {
                        String token = data.get("token").asText();
                        TOKEN_CACHE.put(uid, token);
                        return token;
                    }
                }
            } catch (Exception e) {
                LOG.error("解析登录响应失败, uid: {}", uid, e);
            }
        }
        return null;
    }

    public static String mockUserLogin(int uid) {
        String url = HOST + "/gosh_admin/admin/dev/mock_user_login?uid=" + uid
                + "&did=did-123&lang=en&ctry=in&app=hotya&vsn=3.2.0&ch=google&pf=android&br=redmi&os=Android%2013&mod=21091116c&us=1&seq&adid&gaid&idfa&nw=wifi&ts=1769495283";

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjEsInVzZXJfc291cmNlIjoxLCJleHAiOjE3NzE5OTI3MTB9.czFc8WPy245_bJm0zbrCYE6GLJP1HKoqvYD32mNk38E");
        headers.put("uid", String.valueOf(uid));
        headers.put("us", "1");
        headers.put("zmd", "1");
        headers.put("X-Signature", "{{default_sign}}");
        headers.put("User-Agent", "Apifox/1.0.0 (https://apifox.com)");
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "*/*");

        String jsonBody = "{\"id\": " + uid + "}";

        return HttpUtil.post(url, headers, jsonBody);
    }

    // write to online store
    public static String writeToOnlineStore(FeastRequest request) {
        String token = getToken(LOGIN_UID);
        if (token == null) {
            LOG.error("获取Token失败, uid: {}", LOGIN_UID);
            return null;
        }

        String url = HOST + "/gosh_features/admin/write_to_online_store?uid=" + LOGIN_UID
                + "&did=did-123&lang=en&ctry=in&app=hotya&vsn=3.2.0&ch=google&pf=android&br=redmi&os=Android%2013&mod=21091116c&us=1&seq&adid&gaid&idfa&nw=wifi&ts=1769496861";

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", token);
        headers.put("uid", String.valueOf(LOGIN_UID));
        headers.put("us", "1");
        headers.put("zmd", "1");
        headers.put("X-Signature", "{{default_sign}}");
        headers.put("User-Agent", "Apifox/1.0.0 (https://apifox.com)");
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "*/*");

        try {
            String jsonBody = OBJECT_MAPPER.writeValueAsString(request);
            return HttpUtil.post(url, headers, jsonBody);
        } catch (Exception e) {
            LOG.error("序列化请求体失败", e);
            return null;
        }
    }
}
