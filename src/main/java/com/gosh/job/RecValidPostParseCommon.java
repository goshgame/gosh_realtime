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
            LOG.info("RecValidPostEventParser: raw value={}", value);
            if (value == null || value.isEmpty()) {
                return;
            }

            try {
                JsonNode rootNode = objectMapper.readTree(value);
                LOG.info("RecValidPostEventParser: json value={}", rootNode.toString());

                // 检查event_type
                if (!rootNode.has("event_type")) {
                    LOG.error("RecValidPost 'event_type' is missing in: {}", value);
                    return;
                }

                int eventType = rootNode.get("event_type").asInt();
                if (eventType != recValidPostEventType) {
                    // Not the event type we are interested in
                    LOG.error("RecValidPost 'event_type' is not 17 for value {}", eventType);
                    return;
                }

                JsonNode recValidPostNode = rootNode.path("rec_valid_post_event");
                if (recValidPostNode.isMissingNode()) {
                    LOG.error("RecValidPost 'rec_valid_post_event' is missing in: {}", value);
                    return;
                }

                RecValidPostEvent event = new RecValidPostEvent();
                // ID (uint64, string in JSON)
                JsonNode idNode = recValidPostNode.path("id");
                if (idNode.isMissingNode()) {
                    LOG.error("RecValidPost 'id' is missing in: {}", value);
                    return;
                }
                event.id = Long.parseLong(idNode.asText()); // Parse as string then to long

                // UID (int64)
                event.uid = recValidPostNode.path("uid").asLong(0);
                if (event.uid <= 0) {
                    LOG.error("RecValidPost 'uid' is invalid or missing for id {}: {}", event.id, value);
                    return;
                }

                // TaggingAt (int64)
                event.taggingAt = recValidPostNode.path("tagging_at").asLong(0);
                if (event.taggingAt <= 0) {
                    LOG.error("RecValidPost 'tagging_at' is invalid or missing for id {}: {}", event.id, value);
                    return;
                }
                // tag 时间超过 6 天，则不处理
                if (System.currentTimeMillis() - event.taggingAt * 1000 > 6 * 24 * 60 * 60 * 1000L) { // taggingAt
                                                                                                      // 是秒，需转为毫秒
                    LOG.info("RecValidPost 'tagging_at' is more than 6 days for id {}: {}", event.id, value);
                    return;
                }
                // CreatedAt (int64)
                event.createdAt = recValidPostNode.path("created_at").asLong(0);
                // UpdatedAt (int64)
                event.updatedAt = recValidPostNode.path("updated_at").asLong(0);
                // DeletedAt (int64)
                event.deletedAt = recValidPostNode.path("deleted_at").asLong(0);
                if (event.deletedAt > 0) {
                    LOG.error("RecValidPost 'deleted_at' is not zero for id {}: {}", event.id, value);
                    return;
                }

                // Emit the parsed event
                LOG.info("RecValidPostEventParser: collect event={}", event.toString());
                out.collect(event);

            } catch (Exception e) {
                LOG.error("Failed to parse RecValidPost event: {}", value, e);
            }
        }
    }
}