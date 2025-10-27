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
    private static final int accessLevelLow = 10;
    private static final int accessLevelHigh = 30;
    private static final boolean isDebug = true;

    // ==================== 数据结构定义 ====================
    /**
     * Post打标事件
     */
    public static class PostTagsEvent {
        public long postId;
        public Set<String> contentTagsSet = new HashSet<>();
        public int accessLevel;
        public long createdAt;
        public long updatedAt;
    }

    /**
     * Post基本信息单元事件
     */
    public static class PostInfoEvent {
        public long postId;
        public String tag;
        public int accessLevel;
        public long createdAt;
        public long updatedAt;
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
    public static class PostTagsEventParser implements FlatMapFunction<String, PostTagsEvent> {
        @Override
        public void flatMap(String value, Collector<PostTagsEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                return;
            }

            try {
                //if(isDebug) {
                //    LOG.info("init value: {}", value);
                //}
                JsonNode rootNode = objectMapper.readTree(value);

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    //if(isDebug) {
                    //    LOG.warn("event_type is missing");
                    //}
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                //if(isDebug) {
                //    LOG.info("event_type: {}", eventType);
                //}
                if (eventType != aiTagEventType) {
                    //if(isDebug) {
                    //    LOG.warn("event_type is not equal {}", aiTagEventType);
                    //}
                    return;
                }

                // 检查ai_post_tag_event字段
                JsonNode aiPostTagNode = rootNode.path("ai_post_tag_event");
                if (aiPostTagNode.isMissingNode()) {
                    //if(isDebug) {
                    //    LOG.warn("ai_post_tag_event is missing");
                    //}
                    return;
                }

                // 解析post_id和created_at
                JsonNode postIdNode = aiPostTagNode.path("post_id");
                if (postIdNode.isMissingNode()) {
                    //if(isDebug) {
                    //    LOG.warn("post_id is missing");
                    //}
                    return;
                }
                long postId = postIdNode.asLong();
                //if(isDebug) {
                //    LOG.info("post_id: {}", postId);
                //}
                if (postId <= 0) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {} is invalid", postId);
                    //}
                    return;
                }

                long createdAt = aiPostTagNode.path("created_at").asLong(0); // 使用created_at，因为updated_at在rec中没有获取
                // long updatedAt = aiPostTagNode.path("updated_at").asLong(0);
                if(isDebug) {
                   // LOG.info("post_id: {}, created_at: {}", postId, createdAt);
                    long thresh = System.currentTimeMillis() / 1000 - 86400 * 2;
                    if (createdAt >= thresh) {
                        LOG.info("post_id: {}, created_at: {}, >= thresh: {}", postId, createdAt, thresh);
                    }
                }
                long duration = System.currentTimeMillis() - createdAt * 1000;  // 单位转换
                if (createdAt <= 0 || duration > durationLimitFromCreatedAt) {
                   // if(isDebug) {
                   //     LOG.warn("invalid time. post_id: {}, created_at: {}, duration: {}, " +
                   //             "durationLimitFromCreatedAt: {}", postId, createdAt, duration, durationLimitFromCreatedAt);
                   // }
                   return;
                }

                int accessLevel = aiPostTagNode.path("access_level").asInt(0);
                //if(isDebug) {
                //    LOG.info("post_id: {}, access_level: {}", postId, accessLevel);
                //}
                if (accessLevel < accessLevelLow || accessLevel > accessLevelHigh) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {}, access_level: {} is invalid.", postId, accessLevel);
                    //}
                    return;
                }

                // 解析tags字段
                JsonNode aiTagResultNode = aiPostTagNode.path("results");
                if (aiTagResultNode.isMissingNode()) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {}, results is missing", postId);
                    //}
                    return;
                }

                JsonNode aiTagsNode = aiTagResultNode.path("tags");
                if (aiTagsNode.isMissingNode()) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {}, tags is missing", postId);
                    //}
                    return;
                }

                // 解析content字段
                JsonNode contentTagNode = aiTagsNode.path("content");
                if (contentTagNode.isMissingNode()) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {}, content is missing", postId);
                    //}
                    return;
                }

                Set<String> contentTagSet = new HashSet<>();
                // 处理 content 字段可能的不同格式
                if (contentTagNode.isObject()) {
                    // 单个 key-value 格式: {"key": "value"}
                    Iterator<Map.Entry<String, JsonNode>> fields = contentTagNode.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        contentTagSet.add(field.getKey());
                        //if(isDebug) {
                        //    LOG.info("post_id: {}, contentTag: {}", postId, field.getKey());
                        //}
                    }
                } else if (contentTagNode.isArray()) {
                    // 多个 key-value 组成的 list 格式: [{"key1": "value1"}, {"key2": "value2"}]
                    for (JsonNode item : contentTagNode) {
                        if (item.isObject()) {
                            Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> field = fields.next();
                                contentTagSet.add(field.getKey());
                                //if(isDebug) {
                                //    LOG.info("post_id: {}, contentTag: {}", postId, field.getKey());
                                //}
                            }
                        } else if (item.isTextual()) {
                            // 如果是纯文本标签
                            contentTagSet.add(item.asText());
                        }
                    }
                } else if (contentTagNode.isTextual()) {
                    // 纯文本格式
                    contentTagSet.add(contentTagNode.asText());
                }

                if (contentTagSet.isEmpty()) {
                    //if(isDebug) {
                    //    LOG.warn("post_id: {}, contentTagSet is empty", postId);
                    //}
                    return;
                }

                // 创建并输出事件
                PostTagsEvent event = new PostTagsEvent();
                event.postId = postId;
                event.contentTagsSet = contentTagSet;
                event.createdAt = createdAt;
                event.updatedAt = createdAt;
                event.accessLevel = accessLevel;
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
    public static class PostTagsToPostInfoMapper implements FlatMapFunction<PostTagsEvent, PostInfoEvent> {
        @Override
        public void flatMap(PostTagsEvent postEvent, Collector<PostInfoEvent> out) throws Exception {
            for (String tag : postEvent.contentTagsSet) {
                PostInfoEvent postInfoEvent = new PostInfoEvent();
                postInfoEvent.tag = tag;
                postInfoEvent.createdAt = postEvent.createdAt;
                postInfoEvent.updatedAt = postEvent.updatedAt;
                postInfoEvent.accessLevel = postEvent.accessLevel;
                postInfoEvent.postId = postEvent.postId;
                out.collect(postInfoEvent);

                if(isDebug) {
                    LOG.info("[PostTagsToPostInfoMapper] tag={}, postId={}, createdAt={}, updatedAt={}, accessLevel={}",
                        tag, postEvent.postId, postEvent.createdAt, postEvent.updatedAt, postEvent.accessLevel);
                }
            }
        }
    }

    // ==================== 聚合逻辑 ====================

    /**
     * 标签内容聚合结果
     */
    public static TagPostsAccumulator addEventToAccumulator(PostInfoEvent event, TagPostsAccumulator accumulator) {
        accumulator.tag = event.tag;
        accumulator.postInfos.put(event.postId, event.updatedAt);
        return accumulator;
    }

    public static TagPostsAccumulator mergeAccumulators(TagPostsAccumulator a, TagPostsAccumulator b) {
        // 合并post_id -> updated_at map，取最小值
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
