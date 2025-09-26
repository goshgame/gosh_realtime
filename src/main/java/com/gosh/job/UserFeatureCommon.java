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

    /**
     * 用户特征聚合结果
     */
    public static class UserFeatureAggregation {
        public long uid;
        
        // 1小时特征
        public int viewerExppostCnt1h;
        public int viewerExp1PostCnt1h;  // 图片曝光数
        public int viewerExp2PostCnt1h;  // 视频曝光数
        public int viewer3sviewPostCnt1h;
        public int viewer3sview1PostCnt1h;
        public int viewer3sview2PostCnt1h;
        
        // 历史记录
        public String viewer3sviewPostHis1h;
        public String viewer5sstandPostHis1h;
        public String viewerLikePostHis1h;
        public String viewerFollowPostHis1h;
        public String viewerProfilePostHis1h;
        public String viewerPosinterPostHis1h;
        
        public long updateTime;
    }

    // ==================== 工具函数 ====================

    /**
     * 将事件添加到累加器
     */
    public static UserFeatureAccumulator addEventToAccumulator(UserFeatureEvent event, UserFeatureAccumulator acc) {
        acc.uid = event.uid;

        if ("expose".equals(event.eventType)) {
            acc.exposePostIds.add(event.postId);
            if (event.recToken != null) {
                acc.exposeRecTokens.add(event.recToken);
            }
        }

        if ("view".equals(event.eventType)) {
            acc.viewPostIds.add(event.postId);
            if (event.recToken != null) {
                acc.viewRecTokens.add(event.recToken);
            }

            // 按类型统计
            if (event.postType == 1) {
                acc.view1PostIds.add(event.postId);
            } else if (event.postType == 2) {
                acc.view2PostIds.add(event.postId);
            }

            // 3秒观看
            if (event.progressTime >= 3) {
                acc.view3sPostIds.add(event.postId);
                acc.view3sPostDetails.put(event.postId, event.progressTime);
                if (event.postType == 1) {
                    acc.view3s1PostIds.add(event.postId);
                } else if (event.postType == 2) {
                    acc.view3s2PostIds.add(event.postId);
                }
            }

            // 5秒停留
            if (event.standingTime >= 5) {
                acc.stand5sPostIds.add(event.postId);
                acc.stand5sPostDetails.put(event.postId, event.standingTime);
            }

            // 交互行为
            if (event.interaction != null) {
                for (Integer it : event.interaction) {
                    if (it == 1) { // 点赞
                        acc.likePostIds.add(event.postId);
                        acc.likeAuthors.add(event.author);
                    } else if (it == 2) { // 关注
                        acc.followPostIds.add(event.postId);
                        acc.followAuthors.add(event.author);
                    } else if (it == 3) { // 点主页
                        acc.profilePostIds.add(event.postId);
                        acc.profileAuthors.add(event.author);
                    } else if (it >= 4) { // 评论/收藏/分享
                        acc.posinterPostIds.add(event.postId);
                    }
                }
            }
        }

        return acc;
    }

    /**
     * 合并两个累加器
     */
    public static UserFeatureAccumulator mergeAccumulators(UserFeatureAccumulator a, UserFeatureAccumulator b) {
        a.exposePostIds.addAll(b.exposePostIds);
        a.exposeRecTokens.addAll(b.exposeRecTokens);
        a.viewPostIds.addAll(b.viewPostIds);
        a.viewRecTokens.addAll(b.viewRecTokens);
        a.view1PostIds.addAll(b.view1PostIds);
        a.view2PostIds.addAll(b.view2PostIds);
        a.view3sPostIds.addAll(b.view3sPostIds);
        a.view3s1PostIds.addAll(b.view3s1PostIds);
        a.view3s2PostIds.addAll(b.view3s2PostIds);
        a.view3sPostDetails.putAll(b.view3sPostDetails);
        a.stand5sPostIds.addAll(b.stand5sPostIds);
        a.stand5sPostDetails.putAll(b.stand5sPostDetails);
        a.likePostIds.addAll(b.likePostIds);
        a.followPostIds.addAll(b.followPostIds);
        a.profilePostIds.addAll(b.profilePostIds);
        a.posinterPostIds.addAll(b.posinterPostIds);
        a.likeAuthors.addAll(b.likeAuthors);
        a.followAuthors.addAll(b.followAuthors);
        a.profileAuthors.addAll(b.profileAuthors);
        return a;
    }

    /**
     * 构建带时长的post历史字符串
     */
    public static String buildPostHistoryString(Map<Long, Float> postDetails, int maxLength) {
        if (postDetails.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Map.Entry<Long, Float> entry : postDetails.entrySet()) {
            if (count >= maxLength) break;
            if (count > 0) sb.append(",");
            sb.append(entry.getKey()).append(":").append(String.format("%.1f", entry.getValue()));
            count++;
        }
        return sb.toString();
    }

    /**
     * 构建post列表字符串
     */
    public static String buildPostListString(Set<Long> postIds, int maxLength) {
        if (postIds.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (Long postId : postIds) {
            if (count >= maxLength) break;
            if (count > 0) sb.append(",");
            sb.append(postId);
            count++;
        }
        return sb.toString();
    }

    // ==================== 解析器 ====================

    /**
     * 曝光事件解析器
     */
    public static class ExposeEventParser implements FlatMapFunction<String, PostExposeEvent> {
        @Override
        public void flatMap(String value, Collector<PostExposeEvent> out) throws Exception {
            try {
                JsonNode root = objectMapper.readTree(value);
                if (root.get("event_type").asInt() != 16) {
                    return;
                }

                PostExposeEvent event = new PostExposeEvent();
                event.uid = root.get("uid").asLong();
                event.createdAt = root.get("created_at").asLong();
                event.infoList = new ArrayList<>();

                JsonNode infoList = root.get("info_list");
                if (infoList != null && infoList.isArray()) {
                    for (JsonNode info : infoList) {
                        PostExposeInfo exposeInfo = new PostExposeInfo();
                        exposeInfo.postId = info.get("post_id").asLong();
                        exposeInfo.exposedPos = info.get("exposed_pos").asInt();
                        exposeInfo.expoTime = info.get("expo_time").asLong();
                        exposeInfo.recToken = info.get("rec_token").asText();
                        event.infoList.add(exposeInfo);
                    }
                }

                out.collect(event);
            } catch (Exception e) {
                LOG.error("解析曝光事件失败: {}", e.getMessage());
            }
        }
    }

    /**
     * 观看事件解析器
     */
    public static class ViewEventParser implements FlatMapFunction<String, PostViewEvent> {
        @Override
        public void flatMap(String value, Collector<PostViewEvent> out) throws Exception {
            try {
                JsonNode root = objectMapper.readTree(value);
                if (root.get("event_type").asInt() != 8) {
                    return;
                }

                PostViewEvent event = new PostViewEvent();
                event.uid = root.get("uid").asLong();
                event.createdAt = root.get("created_at").asLong();
                event.infoList = new ArrayList<>();

                JsonNode infoList = root.get("info_list");
                if (infoList != null && infoList.isArray()) {
                    for (JsonNode info : infoList) {
                        PostViewInfo viewInfo = new PostViewInfo();
                        viewInfo.postId = info.get("post_id").asLong();
                        viewInfo.postType = info.get("post_type").asInt();
                        viewInfo.standingTime = (float) info.get("standing_time").asDouble();
                        viewInfo.progressTime = (float) info.get("progress_time").asDouble();
                        viewInfo.author = info.get("author").asLong();
                        viewInfo.viewer = info.get("viewer").asLong();
                        viewInfo.recToken = info.get("rec_token").asText();

                        JsonNode interaction = info.get("interaction");
                        if (interaction != null && interaction.isArray()) {
                            viewInfo.interaction = new ArrayList<>();
                            for (JsonNode it : interaction) {
                                viewInfo.interaction.add(it.asInt());
                            }
                        }

                        event.infoList.add(viewInfo);
                    }
                }

                out.collect(event);
            } catch (Exception e) {
                LOG.error("解析观看事件失败: {}", e.getMessage());
            }
        }
    }

    /**
     * 曝光事件转换为特征事件
     */
    public static class ExposeToFeatureMapper implements FlatMapFunction<PostExposeEvent, UserFeatureEvent> {
        @Override
        public void flatMap(PostExposeEvent value, Collector<UserFeatureEvent> out) throws Exception {
            for (PostExposeInfo info : value.infoList) {
                UserFeatureEvent event = new UserFeatureEvent();
                event.uid = value.uid;
                event.postId = info.postId;
                event.eventType = "expose";
                event.timestamp = value.createdAt;
                event.recToken = info.recToken;
                out.collect(event);
            }
        }
    }

    /**
     * 观看事件转换为特征事件
     */
    public static class ViewToFeatureMapper implements FlatMapFunction<PostViewEvent, UserFeatureEvent> {
        @Override
        public void flatMap(PostViewEvent value, Collector<UserFeatureEvent> out) throws Exception {
            for (PostViewInfo info : value.infoList) {
                UserFeatureEvent event = new UserFeatureEvent();
                event.uid = value.uid;
                event.postId = info.postId;
                event.postType = info.postType;
                event.author = info.author;
                event.eventType = "view";
                event.timestamp = value.createdAt;
                event.standingTime = info.standingTime;
                event.progressTime = info.progressTime;
                event.interaction = info.interaction;
                event.recToken = info.recToken;
                out.collect(event);
            }
        }
    }
}