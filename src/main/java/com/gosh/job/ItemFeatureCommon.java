package com.gosh.job;

import com.gosh.job.UserFeatureCommon.*;
import java.util.*;

/**
 * Item侧特征处理的通用数据结构和工具函数
 */
public class ItemFeatureCommon {

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
        
        // 曝光相关
        public Set<String> exposeRecTokens = new HashSet<>(); // 用于去重计数
        
        // 观看相关 - 不同时长的观看记录，使用recToken去重
        public Set<String> view3sRecTokens = new HashSet<>();  // 3秒以上观看
        public Set<String> view8sRecTokens = new HashSet<>();  // 8秒以上观看
        public Set<String> view12sRecTokens = new HashSet<>(); // 12秒以上观看
        public Set<String> view20sRecTokens = new HashSet<>(); // 20秒以上观看
        
        // 停留相关
        public Set<String> stand5sRecTokens = new HashSet<>();  // 5秒以上停留
        public Set<String> stand10sRecTokens = new HashSet<>(); // 10秒以上停留
        
        // 互动相关
        public Set<String> likeRecTokens = new HashSet<>();     // 点赞
        public Set<String> followRecTokens = new HashSet<>();   // 关注
        public Set<String> profileRecTokens = new HashSet<>();  // 点主页
        public Set<String> posinterRecTokens = new HashSet<>(); // 评论收藏分享
    }

    /**
     * 通用的item特征聚合逻辑
     */
    public static ItemFeatureAccumulator addEventToAccumulator(UserFeatureEvent event, ItemFeatureAccumulator accumulator) {
        accumulator.postId = event.postId;
        
        // 曝光相关特征
        if ("expose".equals(event.eventType)) {
            accumulator.exposeRecTokens.add(event.recToken);
        }
        
        // 观看相关特征
        if ("view".equals(event.eventType)) {
            // 不同时长的观看统计
            if (event.progressTime >= 3) {
                accumulator.view3sRecTokens.add(event.recToken);
            }
            if (event.progressTime >= 8) {
                accumulator.view8sRecTokens.add(event.recToken);
            }
            if (event.progressTime >= 12) {
                accumulator.view12sRecTokens.add(event.recToken);
            }
            if (event.progressTime >= 20) {
                accumulator.view20sRecTokens.add(event.recToken);
            }
            
            // 停留时长统计
            if (event.standingTime >= 5) {
                accumulator.stand5sRecTokens.add(event.recToken);
            }
            if (event.standingTime >= 10) {
                accumulator.stand10sRecTokens.add(event.recToken);
            }
            
            // 互动行为统计
            if (event.interaction != null) {
                for (Integer interactionType : event.interaction) {
                    switch (interactionType) {
                        case 1: // 点赞
                            accumulator.likeRecTokens.add(event.recToken);
                            break;
                        case 13: // 关注
                            accumulator.followRecTokens.add(event.recToken);
                            break;
                        case 15: // 查看主页
                            accumulator.profileRecTokens.add(event.recToken);
                            break;
                        case 3: case 5: case 6: // 评论、收藏、分享
                            accumulator.posinterRecTokens.add(event.recToken);
                            break;
                    }
                }
            }
        }
        
        return accumulator;
    }

    /**
     * 合并两个累加器
     */
    public static ItemFeatureAccumulator mergeAccumulators(ItemFeatureAccumulator a, ItemFeatureAccumulator b) {
        // 合并所有Set
        a.exposeRecTokens.addAll(b.exposeRecTokens);
        
        a.view3sRecTokens.addAll(b.view3sRecTokens);
        a.view8sRecTokens.addAll(b.view8sRecTokens);
        a.view12sRecTokens.addAll(b.view12sRecTokens);
        a.view20sRecTokens.addAll(b.view20sRecTokens);
        
        a.stand5sRecTokens.addAll(b.stand5sRecTokens);
        a.stand10sRecTokens.addAll(b.stand10sRecTokens);
        
        a.likeRecTokens.addAll(b.likeRecTokens);
        a.followRecTokens.addAll(b.followRecTokens);
        a.profileRecTokens.addAll(b.profileRecTokens);
        a.posinterRecTokens.addAll(b.posinterRecTokens);
        
        return a;
    }
} 