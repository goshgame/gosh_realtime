package com.gosh.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FeastApi 测试类
 */
public class FeastApiTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeastApiTest.class);

    public static void main(String[] args) {
        testGetToken();
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
}