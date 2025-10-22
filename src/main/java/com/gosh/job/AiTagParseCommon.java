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
    public static class PostTagEvent {
        public long postId;
        public List<String> contentTagsList;
        public Set<String> contentTagsSet;
        public long createdAt;
    }

    /**
     * Post倒排索引事件
     */
    public static class TagPostEvent {
        public String tag;
        public Set<Long> postIds;
        // public Map<String, Set<Long>> tag2PostIds;
    }


    // ==================== 解析器 ====================
    /**
     * 打标事件解析器
     */
    public static class PostTagEventParser implements FlatMapFunction<String, AiTagParseCommon.PostTagEvent> {
        @Override
        public void flatMap(String value, Collector<AiTagParseCommon.PostTagEvent> out) throws Exception {
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

                List<String> contentTagsList = new ArrayList<>();
                Set<String> contentTagSet = new HashSet<>();
                for(JsonNode node : contentTagNode) {
                    contentTagsList.add(node.asText());
                    contentTagSet.add(node.asText());
                }

                if (contentTagsList.isEmpty()) {
                    return;
                }

                // 创建并输出事件
                PostTagEvent event = new PostTagEvent();
                event.postId = postId;
                event.contentTagsSet = contentTagSet;
                event.contentTagsList = contentTagsList;
                event.createdAt = createdAt;
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse expose event", e);
            }
        }
    }


    // ==================== 事件转换器 ====================

    /**
     * 将打标事件转换为倒排索引事件
     */
    public static class PostToTagMapper implements FlatMapFunction<AiTagParseCommon.PostTagEvent, AiTagParseCommon.TagPostEvent> {
        @Override
        public void flatMap(AiTagParseCommon.PostTagEvent postEvent, Collector<AiTagParseCommon.TagPostEvent> out) throws Exception {
            TagPostEvent tagPostEvent = new TagPostEvent();
            for (String tag : postEvent.contentTagsList) {
                tagPostEvent.tag = tag;
                tagPostEvent.postIds.add(postEvent.postId);
            }
            out.collect(tagPostEvent);
        }
    }

    // ==================== 聚合逻辑 ====================

    // 在 AiTagParseCommon.java 中添加以下类和方法

    /**
     * 标签内容聚合结果
     */
    public static class TagPostsAggregation {
        public String tagId;
        public Set<Long> postIds = new HashSet<>();

        public TagPostsAggregation() {}

    }

    /**
     * 标签内容聚合器
     */
    public static class TagPostsAggregator implements AggregateFunction<TagPostEvent, TagPostsAggregation, TagPostsAggregation> {
        @Override
        public TagPostsAggregation createAccumulator() {
            return new TagPostsAggregation();
        }

        @Override
        public TagPostsAggregation add(AiTagParseCommon.TagPostEvent event, TagPostsAggregation accumulator) {
            if (event.tag != null) {
                accumulator.postIds.addAll(event.postIds);
            }
            return accumulator;
        }

        @Override
        public TagPostsAggregation getResult(TagPostsAggregation accumulator) {
            return accumulator;
        }

        @Override
        public TagPostsAggregation merge(TagPostsAggregation a, TagPostsAggregation b) {
            if (a.tagId == null && b.tagId != null) {
                a.tagId = b.tagId;
            }
            a.postIds.addAll(b.postIds);
            return a;
        }
    }



}
