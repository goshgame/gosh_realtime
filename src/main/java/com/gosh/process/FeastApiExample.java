package com.gosh.process;

import com.gosh.entity.ApiResponse;
import com.gosh.entity.FeastRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FeastApi 使用示例 - 展示响应解析
 */
public class FeastApiExample {
    private static final Logger LOG = LoggerFactory.getLogger(FeastApiExample.class);

    public static void main(String[] args) {
        // 构建请求体
        FeastRequest request = buildFeastRequest();

        // 方式 1: 获取原始 JSON 字符串（保留向后兼容）
        String jsonResponse = FeastApi.writeToOnlineStore(request);
        LOG.info("原始响应: {}", jsonResponse);

        // 方式 2: 获取解析后的结构化响应（推荐）
        ApiResponse<Map<String, Object>> response = FeastApi.writeToOnlineStoreWithResponse(request);

        if (response == null) {
            LOG.error("API 请求失败，响应为空");
            return;
        }

        if (response.isSuccess()) {
            // 成功处理
            Integer code = response.getCode();
            Map<String, Object> data = response.getData();
            Long serverTime = response.getServerTime();

            LOG.info("✅ API 请求成功");
            LOG.info("  - code: {}", code);
            LOG.info("  - data: {}", data);
            LOG.info("  - serverTime: {}", serverTime);

            // 根据 data 内容处理业务逻辑
            if (data != null) {
                data.forEach((key, value) -> {
                    LOG.info("  - data[{}]: {}", key, value);
                });
            }
        } else {
            // 失败处理
            LOG.error("❌ API 请求失败");
            LOG.error("  - code: {}", response.getCode());
            LOG.error("  - data: {}", response.getData());
            LOG.error("  - serverTime: {}", response.getServerTime());
        }
    }

    /**
     * 构建示例 Feast 请求
     */
    private static FeastRequest buildFeastRequest() {
        FeastRequest request = new FeastRequest();
        request.setProject("my_project");
        request.setFeatureViewName("user_features");
        request.setTtl(3600);

        // 构建数据列表
        List<FeastRequest.FeastData> dataList = new ArrayList<>();
        FeastRequest.FeastData data = new FeastRequest.FeastData();

        // 设置 entity key
        FeastRequest.EntityKey entityKey = new FeastRequest.EntityKey();
        List<String> joinKeys = new ArrayList<>();
        joinKeys.add("user_id");
        entityKey.setJoinKeys(joinKeys);

        List<Object> entityValues = new ArrayList<>();
        entityValues.add("user_123");
        entityKey.setEntityValues(entityValues);
        data.setEntityKey(entityKey);

        // 设置特征值
        Map<String, FeastRequest.FeatureValue> features = new HashMap<>();
        FeastRequest.FeatureValue featureValue = new FeastRequest.FeatureValue();
        featureValue.setValueType(FeastRequest.ValueType.INT64);
        featureValue.setInt64Val(1000L);
        features.put("age", featureValue);
        data.setFeatures(features);
        data.setEventTimestamp(System.currentTimeMillis() / 1000);

        dataList.add(data);
        request.setData(dataList);

        return request;
    }
}
