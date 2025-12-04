package com.gosh.entity;

import lombok.Getter;

/**
 * 帖子互动类型枚举
 */
@Getter
public enum PostInteractionType {

    /**
     * 未知
     */
    POST_INTERACTION_NONE(0, "未知"),

    /**
     * 点赞
     */
    POST_INTERACTION_LIKE(1, "点赞"),

    /**
     * 取消点赞
     */
    POST_INTERACTION_UNLIKE(2, "取消点赞"),

    /**
     * 评论
     */
    POST_INTERACTION_COMMENT(3, "评论"),

    /**
     * 删除评论
     */
    POST_INTERACTION_DEL_COMMENT(4, "删除评论"),

    /**
     * 分享 同个Post可多次分享
     */
    POST_INTERACTION_SHARE(5, "分享"),

    /**
     * 收藏
     */
    POST_INTERACTION_MARK(6, "收藏"),

    /**
     * 举报
     */
    POST_INTERACTION_REPORT_POST(7, "举报"),

    /**
     * 举报评论
     */
    POST_INTERACTION_REPORT_COMMENT(8, "举报评论"),

    /**
     * 下载
     */
    POST_INTERACTION_DOWNLOAD(9, "下载"),

    /**
     * 购买
     */
    POST_INTERACTION_PURCHASE(10, "购买"),

    /**
     * 不感兴趣
     */
    POST_INTERACTION_NOT_INTERESTED(11, "不感兴趣"),

    /**
     * 取消收藏
     */
    POST_INTERACTION_UNMARK(12, "取消收藏"),

    /**
     * 关注或点进主页关注
     */
    POST_INTERACTION_FOLLOW(13, "关注"),

    /**
     * 取消关注或点进主页取消关注
     */
    POST_INTERACTION_UNFOLLOW(14, "取消关注"),

    /**
     * 查看主页
     */
    POST_INTERACTION_USER_PROFILE(15, "查看主页"),

    /**
     * 订阅
     */
    POST_INTERACTION_SUBSCRIBE(16, "订阅"),

    /**
     * 取消订阅
     */
    POST_INTERACTION_CANCEL_SUBSCRIBE(17, "取消订阅"),

    /**
     * 减少该作者推荐
     */
    POST_INTERACTION_LESS_FROM_CREATOR(18, "减少该作者推荐");

    private final int value;
    private final String description;

    PostInteractionType(int value, String description) {
        this.value = value;
        this.description = description;
    }

    /**
     * 根据整数值获取枚举
     */
    public static PostInteractionType fromValue(int value) {
        for (PostInteractionType type : PostInteractionType.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return POST_INTERACTION_NONE;
    }

    /**
     * 根据名称获取枚举（忽略大小写）
     */
    public static PostInteractionType fromName(String name) {
        for (PostInteractionType type : PostInteractionType.values()) {
            if (type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        return POST_INTERACTION_NONE;
    }
}
