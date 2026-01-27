package com.gosh.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gosh.entity.ApiResponse;
import com.gosh.entity.FeastRequest;

/**
 * FeastApi 测试类
 */
public class FeastApiTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeastApiTest.class);

    public static void main(String[] args) {
        // testGetToken();
        testWriteToOnlineStore();
    }

    public static void testGetToken() {
        int uid = 1; // 测试 UID
        LOG.info("开始测试 getToken, uid: {}", uid);

        // 第一次调用，应该触发 HTTP 请求
        long start = System.currentTimeMillis();
        String token1 = FeastApi.getToken(uid);
        long end = System.currentTimeMillis();
        LOG.info("第一次获取 token: {}, 耗时: {} ms", token1, (end - start));

        if (token1 != null) {
            LOG.info("第一次获取成功");
        } else {
            LOG.error("第一次获取失败");
        }

        // 第二次调用，应该走缓存
        start = System.currentTimeMillis();
        String token2 = FeastApi.getToken(uid);
        end = System.currentTimeMillis();
        LOG.info("第二次获取 token: {}, 耗时: {} ms", token2, (end - start));

        if (token1 != null && token1.equals(token2)) {
            LOG.info("缓存验证成功，Token一致");
        } else {
            LOG.error("缓存验证失败");
        }
    }

    public static void testWriteToOnlineStore() {
        FeastRequest request = new FeastRequest();
        request.setProject("gosh_feature_store");
        request.setFeatureViewName("post_stats_7d");
        List<FeastRequest.FeastData> dataList = new ArrayList<>();
        FeastRequest.EntityKey entityKey = new FeastRequest.EntityKey();
        entityKey.setJoinKeys(List.of("item_id"));
        entityKey.setEntityValues(List.of(399032450000015800L));

        Map<String, FeastRequest.FeatureValue> features = new HashMap<>();
        FeastRequest.FeatureValue postViewCntFeature = new FeastRequest.FeatureValue();
        postViewCntFeature.setValueType(FeastRequest.ValueType.INT64);
        postViewCntFeature.setInt64Val(100L);
        features.put("post_view_cnt_7d", postViewCntFeature);

        FeastRequest.FeastData feastData = new FeastRequest.FeastData();
        feastData.setEntityKey(entityKey);
        feastData.setFeatures(features);
        feastData.setEventTimestamp(1769502221L);

        dataList.add(feastData);

        request.setData(dataList);
        request.setTtl(3600);
        ApiResponse<Map<String, Object>> response = FeastApi.writeToOnlineStore(request);
        LOG.info("写入在线存储响应: {}", response.toString());
    }
}