package com.gosh.job;

import com.gosh.job.UserFeatureCommon.*;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.*;

/**
 * Item侧特征处理的通用数据结构和工具函数
 */
public class ItemFeatureCommon {
    // HyperLogLog的log2m参数，控制精度
    // log2m=14时，标准误差约为0.81%
    // 使用的内存约为2^14 * 6 / 8 字节 = 12KB
    private static final int HLL_LOG2M = 14;

    /**
     * Redis数据结构，包含key和value
     */
    public static class RedisData {
        public String key;
        public byte[] value;

        public RedisData(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() { return key; }
        public byte[] getValue() { return value; }
    }

    /**
     * Item特征累加器
     */
    public static class ItemFeatureAccumulator {
        public long postId;
        public int totalEventCount = 0;
        
        // 使用HyperLogLog替代HashSet，提供近似去重计数
        // 曝光相关
        public HyperLogLog exposeHLL = new HyperLogLog(HLL_LOG2M);
        
        // 观看相关 - 不同时长的观看记录
        public HyperLogLog view3sHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog view8sHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog view12sHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog view20sHLL = new HyperLogLog(HLL_LOG2M);
        
        // 停留相关
        public HyperLogLog stand5sHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog stand10sHLL = new HyperLogLog(HLL_LOG2M);
        
        // 互动相关
        public HyperLogLog likeHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog followHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog profileHLL = new HyperLogLog(HLL_LOG2M);
        public HyperLogLog posinterHLL = new HyperLogLog(HLL_LOG2M);
    }

    /**
     * 通用的item特征聚合逻辑
     */
    public static ItemFeatureAccumulator addEventToAccumulator(UserFeatureEvent event, ItemFeatureAccumulator accumulator) {
        // 先设置postId，确保即使跳过也能写入Redis
        accumulator.postId = event.postId;
        
        // 曝光相关特征
        if ("expose".equals(event.eventType)) {
            accumulator.exposeHLL.offer(event.recToken);
        }
        
        // 观看相关特征
        if ("view".equals(event.eventType)) {
            // 不同时长的观看统计
            if (event.progressTime >= 3) {
                accumulator.view3sHLL.offer(event.recToken);
            }
            if (event.progressTime >= 8) {
                accumulator.view8sHLL.offer(event.recToken);
            }
            if (event.progressTime >= 12) {
                accumulator.view12sHLL.offer(event.recToken);
            }
            if (event.progressTime >= 20) {
                accumulator.view20sHLL.offer(event.recToken);
            }
            
            // 停留时长统计
            if (event.standingTime >= 5) {
                accumulator.stand5sHLL.offer(event.recToken);
            }
            if (event.standingTime >= 10) {
                accumulator.stand10sHLL.offer(event.recToken);
            }
            
            // 互动行为统计
            if (event.interaction != null) {
                for (Integer interactionType : event.interaction) {
                    switch (interactionType) {
                        case 1: // 点赞
                            accumulator.likeHLL.offer(event.recToken);
                            break;
                        case 13: // 关注
                            accumulator.followHLL.offer(event.recToken);
                            break;
                        case 15: // 查看主页
                            accumulator.profileHLL.offer(event.recToken);
                            break;
                        case 3: case 5: case 6: // 评论、收藏、分享
                            accumulator.posinterHLL.offer(event.recToken);
                            break;
                    }
                }
            }
        }
        
        accumulator.totalEventCount++;
        return accumulator;
    }

    /**
     * 合并两个累加器
     */
    public static ItemFeatureAccumulator mergeAccumulators(ItemFeatureAccumulator a, ItemFeatureAccumulator b) {
        try {
            // 合并所有HLL
            a.exposeHLL = (HyperLogLog) a.exposeHLL.merge(b.exposeHLL);
            
            a.view3sHLL = (HyperLogLog) a.view3sHLL.merge(b.view3sHLL);
            a.view8sHLL = (HyperLogLog) a.view8sHLL.merge(b.view8sHLL);
            a.view12sHLL = (HyperLogLog) a.view12sHLL.merge(b.view12sHLL);
            a.view20sHLL = (HyperLogLog) a.view20sHLL.merge(b.view20sHLL);
            
            a.stand5sHLL = (HyperLogLog) a.stand5sHLL.merge(b.stand5sHLL);
            a.stand10sHLL = (HyperLogLog) a.stand10sHLL.merge(b.stand10sHLL);
            
            a.likeHLL = (HyperLogLog) a.likeHLL.merge(b.likeHLL);
            a.followHLL = (HyperLogLog) a.followHLL.merge(b.followHLL);
            a.profileHLL = (HyperLogLog) a.profileHLL.merge(b.profileHLL);
            a.posinterHLL = (HyperLogLog) a.posinterHLL.merge(b.posinterHLL);
            
            // 合并事件计数
            a.totalEventCount += b.totalEventCount;
        } catch (Exception e) {
            // 如果合并失败，记录错误并返回a
            System.err.println("Error merging HyperLogLog: " + e.getMessage());
        }
        return a;
    }
} 