package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AiTagParseCommon {
    private static final Logger LOG = LoggerFactory.getLogger(AiTagParseCommon.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int aiTagEventType = 11;
    private static final int durationLimitFromCreatedAt = 2 * 24 * 60 * 60 * 1000;  // 2天

    // ==================== 数据结构定义 ====================
    /**
     * Post打标事件
     */
    public static class PostTagsEvent {
        public long postId;
        public Set<String> contentTagsSet = new HashSet<>();
        public long createdAt;
    }

    /**
     * Post基本信息单元事件
     */
    public static class PostInfoEvent {
        public long postId;
        public String tag; 
        public long createdAt;
    }

    /**
     * Tag-posts倒排索引累加器
     */
    public static class TagPostsAccumulator {
        public String tag;
        // 事件总数计数器
        public int totalEventCount = 0;
        // 是否超过事件限制的标记
        public boolean exceededLimit = false;
        // post_id -> created_at
        public Map<Long, Long> postInfos = new HashMap<>();
    }






    // ==================== 解析器 ====================
    /**
     * 打标事件解析器
     */
    public static class PostTagsEventParser implements FlatMapFunction<String, AiTagParseCommon.PostTagsEvent> {
        @Override
        public void flatMap(String value, Collector<AiTagParseCommon.PostTagsEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                return;
            }

            try {
                JsonNode rootNode = objectMapper.readTree(value);

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                if (eventType != aiTagEventType) {
                    return;
                }

                // 检查ai_post_tag_event字段
                JsonNode aiPostTagNode = rootNode.path("ai_post_tag_event");
                if (aiPostTagNode.isMissingNode()) {
                    return;
                }

                // 解析post_id和created_at
                JsonNode postIdNode = aiPostTagNode.path("post_id");
                if (postIdNode.isMissingNode()) {
                    return;
                }
                long postId = postIdNode.asLong();
                if (postId <= 0) {
                    return;
                }

                long createdAt = aiPostTagNode.path("created_at").asLong(0);
                long duration = System.currentTimeMillis() - createdAt;
                if (createdAt <= 0 || duration > durationLimitFromCreatedAt) {
                    return;
                }

                // 解析tags字段
                JsonNode aiTagResultNode = aiPostTagNode.path("results");
                if (aiTagResultNode.isMissingNode()) {
                    return;
                }

                JsonNode aiTagsNode = aiTagResultNode.path("tags");
                if (aiTagsNode.isMissingNode()) {
                    return;
                }

                // 解析content字段
                JsonNode contentTagNode = aiTagsNode.path("content");
                if (contentTagNode.isMissingNode() || !contentTagNode.isArray()) {
                    return;
                }

                Set<String> contentTagSet = new HashSet<>();
                for(JsonNode node : contentTagNode) {
                    contentTagSet.add(node.asText());
                }

                if (contentTagSet.isEmpty()) {
                    return;
                }

                // 创建并输出事件
                PostTagsEvent event = new PostTagsEvent();
                event.postId = postId;
                event.contentTagsSet = contentTagSet;
                event.createdAt = createdAt;
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse expose event", e);
            }
        }
    }


    // ==================== 事件转换器 ====================

    /**
     * 将打标事件转换为基本信息单元事件
     */
    public static class PostTagsToPostInfoMapper implements FlatMapFunction<AiTagParseCommon.PostTagsEvent, AiTagParseCommon.PostInfoEvent> {
        @Override
        public void flatMap(AiTagParseCommon.PostTagsEvent postEvent, Collector<AiTagParseCommon.PostInfoEvent> out) throws Exception {
            for (String tag : postEvent.contentTagsSet) {
                PostInfoEvent postInfoEvent = new PostInfoEvent();
                postInfoEvent.tag = tag;
                postInfoEvent.createdAt = postEvent.createdAt;
                postInfoEvent.postId = postEvent.postId;
                out.collect(postInfoEvent);
            }
        }
    }

    // ==================== 聚合逻辑 ====================

    /**
     * 标签内容聚合结果
     */

    public static TagPostsAccumulator addEventToAccumulator(PostInfoEvent event, TagPostsAccumulator accumulator) {
        accumulator.tag = event.tag;
        accumulator.postInfos.put(event.postId, event.createdAt);
        return accumulator;
    }

    public static TagPostsAccumulator mergeAccumulators(TagPostsAccumulator a, TagPostsAccumulator b) {
        // 合并post_id -> created_at map，取最小值
        for (Map.Entry<Long, Long> entry : b.postInfos.entrySet()) {
            a.postInfos.merge(entry.getKey(), entry.getValue(), Long::min);
        }
        return a;
    }


    // ==================== 工具函数 ====================
    /**
     * 构建post和createdAt历史字符串 格式: "123|5,234|10"
     */
    public static String buildPostCreatedAtString(Map<Long, Long> postInfos, int limit) {
        return postInfos.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                .limit(limit)
                .map(entry -> entry.getKey() + "|" + entry.getValue())
                .reduce((a, b) -> a + "," + b)
                .orElse("");
    }


}
