package com.gosh.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 事件类型过滤工具类
 * 提供高效的事件过滤功能，先进行字符串预过滤，再进行JSON解析确认
 */
public class EventFilterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EventFilterUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 创建单一事件类型过滤器
     * @param eventType 目标事件类型
     * @return Flink FilterFunction
     */
    public static FilterFunction<String> createEventTypeFilter(int eventType) {
        return new SingleEventTypeFilter(eventType);
    }

    /**
     * 创建多事件类型过滤器
     * @param eventTypes 目标事件类型数组
     * @return Flink FilterFunction
     */
    public static FilterFunction<String> createEventTypeFilter(int... eventTypes) {
        return new MultiEventTypeFilter(eventTypes);
    }

    /**
     * 快速字符串检查 - 检查JSON字符串是否包含指定的event_type
     * @param jsonString JSON字符串
     * @param eventType 事件类型
     * @return 是否可能包含该事件类型
     */
    public static boolean quickContainsEventType(String jsonString, int eventType) {
        if (jsonString == null || jsonString.isEmpty()) {
            return false;
        }
        
        String basePattern = "\"event_type\":" + eventType;
        int index = jsonString.indexOf(basePattern);
        
        if (index == -1) {
            return false;
        }
        
        // 检查匹配位置后的字符，确保后面不是数字（避免1匹配到10、11等）
        int nextCharIndex = index + basePattern.length();
        if (nextCharIndex < jsonString.length()) {
            char nextChar = jsonString.charAt(nextCharIndex);
            // 如果下一个字符是数字，说明是部分匹配
            return !Character.isDigit(nextChar);
        }
        
        // 如果已经到字符串末尾，说明匹配成功
        return true;
    }

    /**
     * 快速字符串检查 - 检查JSON字符串是否包含任意指定的event_type
     * @param jsonString JSON字符串
     * @param eventTypes 事件类型数组
     * @return 是否可能包含任意一个事件类型
     */
    public static boolean quickContainsAnyEventType(String jsonString, int... eventTypes) {
        if (jsonString == null || jsonString.isEmpty()) {
            return false;
        }
        for (int eventType : eventTypes) {
            if (quickContainsEventType(jsonString, eventType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 精确JSON解析检查 - 通过解析JSON确认event_type
     * @param jsonString JSON字符串
     * @param eventType 目标事件类型
     * @return 是否确实包含该事件类型
     */
    public static boolean exactContainsEventType(String jsonString, int eventType) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            return rootNode.has("event_type") && rootNode.get("event_type").asInt() == eventType;
        } catch (Exception e) {
            LOG.debug("Failed to parse JSON for event type check: {}", jsonString, e);
            return false;
        }
    }

    /**
     * 精确JSON解析检查 - 通过解析JSON确认是否包含任意指定的event_type
     * @param jsonString JSON字符串
     * @param eventTypes 事件类型数组
     * @return 是否确实包含任意一个事件类型
     */
    public static boolean exactContainsAnyEventType(String jsonString, int... eventTypes) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            if (!rootNode.has("event_type")) {
                return false;
            }
            int actualEventType = rootNode.get("event_type").asInt();
            return Arrays.stream(eventTypes).anyMatch(type -> type == actualEventType);
        } catch (Exception e) {
            LOG.debug("Failed to parse JSON for event type check: {}", jsonString, e);
            return false;
        }
    }

    /**
     * 获取JSON中的事件类型
     * @param jsonString JSON字符串
     * @return 事件类型，如果解析失败返回-1
     */
    public static int getEventType(String jsonString) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            return rootNode.has("event_type") ? rootNode.get("event_type").asInt() : -1;
        } catch (Exception e) {
            LOG.debug("Failed to parse JSON for event type extraction: {}", jsonString, e);
            return -1;
        }
    }

    /**
     * 单一事件类型过滤器实现
     */
    private static class SingleEventTypeFilter implements FilterFunction<String> {
        private final int targetEventType;

        public SingleEventTypeFilter(int targetEventType) {
            this.targetEventType = targetEventType;
        }

        @Override
        public boolean filter(String value) throws Exception {
            // 第一阶段：快速字符串检查
            if (!quickContainsEventType(value, targetEventType)) {
                return false;
            }
            
            // 第二阶段：精确JSON解析确认
            return exactContainsEventType(value, targetEventType);
        }
    }

    /**
     * 多事件类型过滤器实现
     */
    private static class MultiEventTypeFilter implements FilterFunction<String> {
        private final int[] targetEventTypes;
        private final Set<Integer> eventTypeSet;

        public MultiEventTypeFilter(int... targetEventTypes) {
            this.targetEventTypes = targetEventTypes;
            this.eventTypeSet = Arrays.stream(targetEventTypes)
                .boxed()
                .collect(Collectors.toSet());
        }

        @Override
        public boolean filter(String value) throws Exception {
            // 第一阶段：快速字符串检查
            if (!quickContainsAnyEventType(value, targetEventTypes)) {
                return false;
            }
            
            // 第二阶段：精确JSON解析确认
            return exactContainsAnyEventType(value, targetEventTypes);
        }
    }

    /**
     * 高性能事件类型过滤器 - 仅使用字符串匹配，适用于对准确性要求不高但对性能要求极高的场景
     */
    public static class FastEventTypeFilter implements FilterFunction<String> {
        private final int[] targetEventTypes;

        public FastEventTypeFilter(int... targetEventTypes) {
            this.targetEventTypes = targetEventTypes;
        }

        @Override
        public boolean filter(String value) throws Exception {
            // 仅进行快速字符串检查，不进行JSON解析
            return quickContainsAnyEventType(value, targetEventTypes);
        }
    }

    /**
     * 创建高性能事件类型过滤器（仅字符串匹配）
     * @param eventTypes 目标事件类型数组
     * @return 高性能过滤器
     */
    public static FilterFunction<String> createFastEventTypeFilter(int... eventTypes) {
        return new FastEventTypeFilter(eventTypes);
    }
} 