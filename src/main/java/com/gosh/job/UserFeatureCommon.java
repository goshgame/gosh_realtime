package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 用户特征处理的通用数据结构和工具函数
 */
public class UserFeatureCommon {
    private static final Logger LOG = LoggerFactory.getLogger(UserFeatureCommon.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // ==================== 数据结构定义 ====================

    /**
     * Post曝光事件
     */
    public static class PostExposeEvent {
        public long uid;
        public List<PostExposeInfo> infoList;
        public long createdAt;
    }

    /**
     * Post曝光信息
     */
    public static class PostExposeInfo {
        public long postId;
        public int exposedPos;
        public long expoTime;
        public String recToken;
    }

    /**
     * Post观看事件
     */
    public static class PostViewEvent {
        public long uid;
        public List<PostViewInfo> infoList;
        public long createdAt;
    }

    /**
     * Post观看信息
     */
    public static class PostViewInfo {
        public long postId;
        public int postType;
        public float standingTime;
        public float progressTime;
        public long author;
        public long viewer;
        public String recToken;
        public List<Integer> interaction;
    }

    /**
     * 统一的用户特征事件
     */
    public static class UserFeatureEvent {
        public long uid;
        public long postId;
        public int postType;
        public long author;
        public String eventType;
        public long timestamp;
        public float standingTime;
        public float progressTime;
        public List<Integer> interaction;
        public String recToken;

        public long getTimestamp() { return timestamp; }
        public long getUid() { return uid; }
    }

    /**
     * 用户特征累加器
     */
    public static class UserFeatureAccumulator {
        public long uid;

        // 曝光相关
        public Set<Long> exposePostIds = new HashSet<>();
        public Set<String> exposeRecTokens = new HashSet<>();

        // 观看相关
        public Set<Long> viewPostIds = new HashSet<>();
        public Set<String> viewRecTokens = new HashSet<>();
        public Set<Long> view1PostIds = new HashSet<>(); // 图片post
        public Set<Long> view2PostIds = new HashSet<>(); // 视频post

        // 3秒观看
        public Set<Long> view3sPostIds = new HashSet<>();
        public Set<Long> view3s1PostIds = new HashSet<>(); // 3秒图片
        public Set<Long> view3s2PostIds = new HashSet<>(); // 3秒视频
        public Map<Long, Float> view3sPostDetails = new HashMap<>(); // postId -> progressTime

        // 5秒停留
        public Set<Long> stand5sPostIds = new HashSet<>();
        public Map<Long, Float> stand5sPostDetails = new HashMap<>(); // postId -> standingTime

        // 交互行为
        public Set<Long> likePostIds = new HashSet<>();
        public Set<Long> followPostIds = new HashSet<>();
        public Set<Long> profilePostIds = new HashSet<>();
        public Set<Long> posinterPostIds = new HashSet<>();

        // 作者相关
        public Set<Long> likeAuthors = new HashSet<>();
        public Set<Long> followAuthors = new HashSet<>();
        public Set<Long> profileAuthors = new HashSet<>();
    }

    // ==================== 解析器 ====================

    /**
     * 曝光事件解析器
     */
    public static class ExposeEventParser implements FlatMapFunction<String, PostExposeEvent> {
        @Override
        public void flatMap(String value, Collector<PostExposeEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                LOG.warn("Empty input value");
                return;
            }

            try {
                LOG.debug("Processing expose event: {}", value);
                JsonNode rootNode = objectMapper.readTree(value);

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    LOG.warn("Missing event_type field");
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                if (eventType != 16) {
                    LOG.debug("Skipping non-expose event: event_type={}", eventType);
                    return;
                }

                // 检查post_expose字段
                JsonNode exposeNode = rootNode.path("post_expose");
                if (exposeNode.isMissingNode()) {
                    LOG.warn("Missing post_expose field");
                    return;
                }

                // 解析uid和created_at
                JsonNode uidNode = exposeNode.path("uid");
                if (uidNode.isMissingNode()) {
                    LOG.warn("Missing uid field");
                    return;
                }
                long uid = uidNode.asLong();
                if (uid <= 0) {
                    LOG.warn("Invalid uid: {}", uid);
                    return;
                }

                long createdAt = exposeNode.path("created_at").asLong(0);
                if (createdAt <= 0) {
                    LOG.warn("Invalid created_at: {}", createdAt);
                    return;
                }

                // 解析list字段
                JsonNode listNode = exposeNode.path("list");
                if (listNode.isMissingNode() || !listNode.isArray()) {
                    LOG.warn("Missing or invalid list field");
                    return;
                }

                List<PostExposeInfo> infoList = new ArrayList<>();
                for (JsonNode itemNode : listNode) {
                    try {
                        PostExposeInfo info = new PostExposeInfo();
                        
                        // 解析post_id
                        JsonNode postIdNode = itemNode.path("post_id");
                        if (!postIdNode.isMissingNode()) {
                            String postIdStr = postIdNode.asText();
                            try {
                                info.postId = Long.parseLong(postIdStr);
                            } catch (NumberFormatException e) {
                                LOG.warn("Invalid post_id format: {}", postIdStr);
                                continue;
                            }
                        }

                        if (info.postId <= 0) {
                            LOG.warn("Invalid post_id: {}", info.postId);
                            continue;
                        }

                        // 解析其他字段
                        info.exposedPos = itemNode.path("exposed_pos").asInt(0);
                        info.expoTime = itemNode.path("expo_time").asLong(0);
                        info.recToken = itemNode.path("rec_token").asText("");

                        infoList.add(info);
                        LOG.debug("Parsed expose info: postId={}, recToken={}", info.postId, info.recToken);
                    } catch (Exception e) {
                        LOG.warn("Failed to parse list item: {}", itemNode, e);
                    }
                }

                if (infoList.isEmpty()) {
                    LOG.warn("No valid items found in list");
                    return;
                }

                // 创建并输出事件
                PostExposeEvent event = new PostExposeEvent();
                event.uid = uid;
                event.infoList = infoList;
                event.createdAt = createdAt;
                LOG.debug("Emitting expose event: uid={}, items={}", uid, infoList.size());
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse expose event: {}", value, e);
                LOG.error("Exception details:", e);
            }
        }
    }

    /**
     * 观看事件解析器
     */
    public static class ViewEventParser implements FlatMapFunction<String, PostViewEvent> {
        @Override
        public void flatMap(String value, Collector<PostViewEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                LOG.warn("Empty input value");
                return;
            }

            try {
                LOG.debug("Processing view event: {}", value);
                JsonNode rootNode = objectMapper.readTree(value);

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    LOG.warn("Missing event_type field");
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                if (eventType != 8) {
                    LOG.debug("Skipping non-view event: event_type={}", eventType);
                    return;
                }

                // 检查post_view字段
                JsonNode viewNode = rootNode.path("post_view");
                if (viewNode.isMissingNode()) {
                    LOG.warn("Missing post_view field");
                    return;
                }

                // 解析uid和created_at
                JsonNode uidNode = viewNode.path("uid");
                if (uidNode.isMissingNode()) {
                    LOG.warn("Missing uid field");
                    return;
                }
                long uid = uidNode.asLong();
                if (uid <= 0) {
                    LOG.warn("Invalid uid: {}", uid);
                    return;
                }

                long createdAt = viewNode.path("created_at").asLong(0);
                if (createdAt <= 0) {
                    LOG.warn("Invalid created_at: {}", createdAt);
                    return;
                }

                // 解析list字段
                JsonNode listNode = viewNode.path("list");
                if (listNode.isMissingNode() || !listNode.isArray()) {
                    LOG.warn("Missing or invalid list field");
                    return;
                }

                List<PostViewInfo> infoList = new ArrayList<>();
                for (JsonNode itemNode : listNode) {
                    try {
                        PostViewInfo info = new PostViewInfo();
                        
                        // 解析post_id
                        JsonNode postIdNode = itemNode.path("post_id");
                        if (!postIdNode.isMissingNode()) {
                            String postIdStr = postIdNode.asText();
                            try {
                                info.postId = Long.parseLong(postIdStr);
                            } catch (NumberFormatException e) {
                                LOG.warn("Invalid post_id format: {}", postIdStr);
                                continue;
                            }
                        }

                        if (info.postId <= 0) {
                            LOG.warn("Invalid post_id: {}", info.postId);
                            continue;
                        }

                        // 解析其他字段
                        info.postType = itemNode.path("post_type").asInt(0);
                        info.standingTime = itemNode.path("standing_time").floatValue();
                        info.progressTime = itemNode.path("progress_time").floatValue();
                        info.author = itemNode.path("author").asLong(0);
                        info.viewer = itemNode.path("viewer").asLong(0);
                        info.recToken = itemNode.path("rec_token").asText("");

                        // 解析interaction数组
                        JsonNode interactionNode = itemNode.path("interaction");
                        if (!interactionNode.isMissingNode() && interactionNode.isArray()) {
                            List<Integer> interactions = new ArrayList<>();
                            for (JsonNode intNode : interactionNode) {
                                try {
                                    interactions.add(intNode.asInt());
                                } catch (Exception e) {
                                    LOG.warn("Invalid interaction value: {}", intNode);
                                }
                            }
                            info.interaction = interactions;
                        }

                        infoList.add(info);
                        LOG.debug("Parsed view info: postId={}, postType={}, recToken={}", 
                            info.postId, info.postType, info.recToken);
                    } catch (Exception e) {
                        LOG.warn("Failed to parse list item: {}", itemNode, e);
                    }
                }

                if (infoList.isEmpty()) {
                    LOG.warn("No valid items found in list");
                    return;
                }

                // 创建并输出事件
                PostViewEvent event = new PostViewEvent();
                event.uid = uid;
                event.infoList = infoList;
                event.createdAt = createdAt;
                LOG.debug("Emitting view event: uid={}, items={}", uid, infoList.size());
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse view event: {}", value, e);
                LOG.error("Exception details:", e);
            }
        }
    }

    // ==================== 事件转换器 ====================

    /**
     * 将曝光事件转换为用户特征事件
     */
    public static class ExposeToFeatureMapper implements FlatMapFunction<PostExposeEvent, UserFeatureEvent> {
        @Override
        public void flatMap(PostExposeEvent exposeEvent, Collector<UserFeatureEvent> out) throws Exception {
            for (PostExposeInfo info : exposeEvent.infoList) {
                UserFeatureEvent featureEvent = new UserFeatureEvent();
                featureEvent.uid = exposeEvent.uid;
                featureEvent.postId = info.postId;
                featureEvent.eventType = "expose";
                featureEvent.timestamp = exposeEvent.createdAt * 1000; // 转换为毫秒
                featureEvent.recToken = info.recToken;
                out.collect(featureEvent);
            }
        }
    }

    /**
     * 将观看事件转换为用户特征事件
     */
    public static class ViewToFeatureMapper implements FlatMapFunction<PostViewEvent, UserFeatureEvent> {
        @Override
        public void flatMap(PostViewEvent viewEvent, Collector<UserFeatureEvent> out) throws Exception {
            for (PostViewInfo info : viewEvent.infoList) {
                UserFeatureEvent featureEvent = new UserFeatureEvent();
                featureEvent.uid = viewEvent.uid;
                featureEvent.postId = info.postId;
                featureEvent.postType = info.postType;
                featureEvent.author = info.author;
                featureEvent.eventType = "view";
                featureEvent.timestamp = viewEvent.createdAt * 1000; // 转换为毫秒
                featureEvent.standingTime = info.standingTime;
                featureEvent.progressTime = info.progressTime;
                featureEvent.interaction = info.interaction;
                featureEvent.recToken = info.recToken;
                out.collect(featureEvent);
            }
        }
    }

    // ==================== 聚合逻辑 ====================

    /**
     * 通用的用户特征聚合逻辑
     */
    public static UserFeatureAccumulator addEventToAccumulator(UserFeatureEvent event, UserFeatureAccumulator accumulator) {
        accumulator.uid = event.uid;

        // 曝光相关特征
        if ("expose".equals(event.eventType)) {
            accumulator.exposePostIds.add(event.postId);
            accumulator.exposeRecTokens.add(event.recToken);
        }

        // 观看相关特征
        if ("view".equals(event.eventType)) {
            accumulator.viewPostIds.add(event.postId);
            accumulator.viewRecTokens.add(event.recToken);

            // 按post_type分类统计
            if (event.postType == 1) { // 图片
                accumulator.view1PostIds.add(event.postId);
            } else if (event.postType == 2) { // 视频
                accumulator.view2PostIds.add(event.postId);
            }

            // 3秒以上观看
            if (event.progressTime >= 3) {
                accumulator.view3sPostIds.add(event.postId);
                accumulator.view3sPostDetails.put(event.postId, event.progressTime);

                if (event.postType == 1) {
                    accumulator.view3s1PostIds.add(event.postId);
                } else if (event.postType == 2) {
                    accumulator.view3s2PostIds.add(event.postId);
                }
            }

            // 5秒以上停留
            if (event.standingTime >= 5) {
                accumulator.stand5sPostIds.add(event.postId);
                accumulator.stand5sPostDetails.put(event.postId, event.standingTime);
            }

            // 处理交互行为
            if (event.interaction != null) {
                for (Integer interactionType : event.interaction) {
                    switch (interactionType) {
                        case 1: // 点赞
                            accumulator.likePostIds.add(event.postId);
                            accumulator.likeAuthors.add(event.author);
                            break;
                        case 13: // 关注
                            accumulator.followPostIds.add(event.postId);
                            accumulator.followAuthors.add(event.author);
                            break;
                        case 15: // 查看主页
                            accumulator.profilePostIds.add(event.postId);
                            accumulator.profileAuthors.add(event.author);
                            break;
                        case 3: case 5: case 6: // 评论、收藏、分享
                            accumulator.posinterPostIds.add(event.postId);
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
    public static UserFeatureAccumulator mergeAccumulators(UserFeatureAccumulator a, UserFeatureAccumulator b) {
        a.exposePostIds.addAll(b.exposePostIds);
        a.viewPostIds.addAll(b.viewPostIds);
        a.view1PostIds.addAll(b.view1PostIds);
        a.view2PostIds.addAll(b.view2PostIds);
        a.view3sPostIds.addAll(b.view3sPostIds);
        a.view3s1PostIds.addAll(b.view3s1PostIds);
        a.view3s2PostIds.addAll(b.view3s2PostIds);
        a.stand5sPostIds.addAll(b.stand5sPostIds);
        a.likePostIds.addAll(b.likePostIds);
        a.followPostIds.addAll(b.followPostIds);
        a.profilePostIds.addAll(b.profilePostIds);
        a.posinterPostIds.addAll(b.posinterPostIds);
        a.likeAuthors.addAll(b.likeAuthors);
        a.followAuthors.addAll(b.followAuthors);
        a.profileAuthors.addAll(b.profileAuthors);

        // 合并详情map，取最大值
        for (Map.Entry<Long, Float> entry : b.view3sPostDetails.entrySet()) {
            a.view3sPostDetails.merge(entry.getKey(), entry.getValue(), Float::max);
        }
        for (Map.Entry<Long, Float> entry : b.stand5sPostDetails.entrySet()) {
            a.stand5sPostDetails.merge(entry.getKey(), entry.getValue(), Float::max);
        }

        return a;
    }

    // ==================== 工具函数 ====================

    /**
     * 构建带时长的post历史字符串 格式: "123|5,234|10"
     */
    public static String buildPostHistoryString(Map<Long, Float> postDetails, int limit) {
        return postDetails.entrySet().stream()
            .sorted(Map.Entry.<Long, Float>comparingByValue().reversed())
            .limit(limit)
            .map(entry -> entry.getKey() + "|" + entry.getValue().intValue())
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    }

    /**
     * 构建post列表字符串 格式: "123,234"
     */
    public static String buildPostListString(Set<Long> postIds, int limit) {
        return postIds.stream()
            .limit(limit)
            .map(String::valueOf)
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    }

    /**
     * 构建作者列表字符串 格式: "123,234"
     */
    public static String buildAuthorListString(Set<Long> authorIds, int limit) {
        return authorIds.stream()
            .limit(limit)
            .map(String::valueOf)
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    }
}