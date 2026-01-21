package com.gosh.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * 表示一个经过打标签和审核的有效Post数据结构。
 */
public class RecValidPostParseCommon {
    private static final Logger LOG = LoggerFactory.getLogger(RecValidPostParseCommon.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int recValidPostEventType = 17; // Event type for RecValidPost

    public static class RecValidPostEvent {
        public long id; // 作品ID
        public long uid; // 用户ID
        public long taggingAt; // 打标签时间
        public long createdAt; // 创建时间
        public long updatedAt; // 更新时间
        public long deletedAt; // 删除时间
        public int deletedBy; // 删除者类型 (假设为int枚举)
        public String taggingBy; // 打标签者

        @Override
        public String toString() {
            return "RecValidPost{" + "id=" + id + ", uid=" + uid + ", taggingAt=" + taggingAt + ", createdAt="
                    + createdAt + ", updatedAt=" + updatedAt + ", deletedAt=" + deletedAt + ", deletedBy=" + deletedBy
                    + ", taggingBy='" + taggingBy + '\'' + '}';
        }
    }

    // ==================== 解析器 ====================
    public static class RecValidPostEventParser implements FlatMapFunction<String, RecValidPostEvent> {
        @Override
        public void flatMap(String value, Collector<RecValidPostEvent> out) throws Exception {
            if (value == null || value.isEmpty()) {
                return;
            }

            try {
                JsonNode rootNode = objectMapper.readTree(value);
                LOG.info("RecValidPostEventParser: value={}", rootNode.toString());

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    LOG.warn("RecValidPost 'event_type' is missing in: {}", value);
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                if (eventType != recValidPostEventType) {
                    // Not the event type we are interested in
                    return;
                }

                RecValidPostEvent event = new RecValidPostEvent();

                // ID (uint64, string in JSON)
                JsonNode idNode = rootNode.path("id");
                if (idNode.isMissingNode()) {
                    LOG.warn("RecValidPost 'id' is missing in: {}", value);
                    return;
                }
                event.id = Long.parseLong(idNode.asText()); // Parse as string then to long

                // UID (int64)
                event.uid = rootNode.path("uid").asLong(0);
                if (event.uid <= 0) {
                    LOG.warn("RecValidPost 'uid' is invalid or missing for id {}: {}", event.id, value);
                    return;
                }

                // TaggingAt (int64)
                event.taggingAt = rootNode.path("tagging_at").asLong(0);
                if (event.taggingAt <= 0) {
                    LOG.warn("RecValidPost 'tagging_at' is invalid or missing for id {}: {}", event.id, value);
                    return;
                }
                // tag 时间超过 6 天，则不处理
                if (System.currentTimeMillis() - event.taggingAt * 1000 > 6 * 24 * 60 * 60 * 1000L) { // taggingAt
                                                                                                      // 是秒，需转为毫秒
                    LOG.warn("RecValidPost 'tagging_at' is more than 6 days for id {}: {}", event.id, value);
                    return;
                }
                // CreatedAt (int64)
                event.createdAt = rootNode.path("created_at").asLong(0);
                // UpdatedAt (int64)
                event.updatedAt = rootNode.path("updated_at").asLong(0);
                // DeletedAt (int64)
                event.deletedAt = rootNode.path("deleted_at").asLong(0);
                if (event.deletedAt > 0) {
                    LOG.warn("RecValidPost 'deleted_at' is not zero for id {}: {}", event.id, value);
                    return;
                }

                // Emit the parsed event
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse RecValidPost event: {}", value, e);
            }
        }
    }
}