package com.gosh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.entity.ParsedPostEvent;
import com.gosh.job.AiTagParseCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//public static class PostTagsEvent {
//    public long postId;
//    public Set<String> contentTagsSet = new HashSet<>();
//    public int accessLevel;
//    public long createdAt;
//    public long updatedAt;
//}


public class ContentTagJsonParseDemo {
    private static final Logger LOG = LoggerFactory.getLogger(ContentTagJsonParseDemo.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int aiTagEventType = 11;
    private static final int durationLimitFromCreatedAt = 2 * 24 * 60 * 60 * 1000;  // 2天
    private static final int accessLevelLow = 10;
    private static final int accessLevelHigh = 30;


    public static void PrintParseResult(AiTagParseCommon.PostTagsEvent postEvent) {
        System.out.println("PrintParseResult start...");
        StringBuilder sb = new StringBuilder();
        sb.append("postId: ").append(postEvent.postId).append("\n");
        sb.append("accessLevel: ").append(postEvent.accessLevel).append("\n");
        sb.append("createdAt: ").append(postEvent.createdAt).append("\n");
        sb.append("updatedAt: ").append(postEvent.updatedAt).append("\n");

        for (String tag : postEvent.contentTagsSet) {
            sb.append("contentTagSet: ").append(tag).append("\n");
        }

        System.out.println(sb.toString());
        System.out.println("PrintParseResult done.");
    }


    public static void main(String[] args) {
        System.out.println("ContentTagJsonParseDemo start...");
        // 测试数据
//        String json = """
//                {
//                  "post_id": 328475200000018741,
//                  "results": {
//                    "asr_text": " Had I become a... Darn it!",
//                    "language": "en",
//                    "asr_abstract": null,
//                    "desc": "**Object (Who)**: A young man stands outdoors near a bicycle. He has short dark hair and is wearing a dark jacket over a light-colored shirt, along with dark pants. He appears relaxed and is smiling. He also seems to be wearing a backpack.\\n\\n**What + Where + When (What is happening + Where + When)**: The man is standing outdoors in a natural setting with trees and greenery around him. There is a bicycle leaning against a tree trunk next to him. The timestamp shows it is 11:17 am on Thursday. The man does not appear to be singing or dancing; he is simply standing and smiling. His facial expression suggests he is happy or amused, possibly reacting to the camera or something off-screen. The setting appears to be during the daytime, given the natural light.\\n\\n**Quality**: The video quality is clear, with good visibility and sharpness. The image is well-lit, making the subject easily distinguishable. The camera work is steady, and there is no noticeable shakiness or blurring.\\n\\n**Image Style**: The visual style is naturalistic and casual. The colors are vibrant, with a mix of greens from the trees and earthy tones from the ground and the man's clothing. The overall aesthetic is simple and uncluttered, focusing on the man and his immediate surroundings.\\n\\n**Functiontype & Formtype**: The function of the video is likely to capture a moment of the man's day, perhaps sharing a personal or humorous experience. The form is informal and likely intended for social media platforms, where short clips are common.\\n\\n**Content Restriction Level (restricted)**: The content restriction level is clean and appropriate for all audiences. There are no explicit or inappropriate elements, and the language used is neutral and positive.",
//                    "tags": {
//                      "emotions": "joyful",
//                      "imagestyle": "vibrant",
//                      "occasion": "recreational",
//                      "restricted": "clean",
//                      "functiontype": "entertainment",
//                      "gender": "male",
//                      "quality": "high",
//                      "formtype": "vlog",
//                      "age": "youngadult",
//                      "content": {
//                        "life": "dailyvlog"
//                      },
//                      "scenetype": "outdoor",
//                      "appearance": {
//                        "bodytype": "average",
//                        "hairstyle": "short_hair",
//                        "beauty_level": "medium"
//                      },
//                      "object": "human",
//                      "language": "english"
//                    },
//                    "post_id": 328475200000018750
//                  },
//                  "access_level": 10,
//                  "created_at": 1761291616,
//                  "updated_at": 1761291616
//                }
//        """;

        String json = "{\n" +
                "  \"post_id\": 328475200000018741,\n" +
                "  \"results\": {\n" +
                "    \"asr_text\": \" Had I become a... Darn it!\",\n" +
                "    \"language\": \"en\",\n" +
                "    \"asr_abstract\": null,\n" +
                "    \"desc\": \"**Object (Who)**: A young man stands outdoors near a bicycle. He has short dark hair and is wearing a dark jacket over a light-colored shirt, along with dark pants. He appears relaxed and is smiling. He also seems to be wearing a backpack.\\n\\n**What + Where + When (What is happening + Where + When)**: The man is standing outdoors in a natural setting with trees and greenery around him. There is a bicycle leaning against a tree trunk next to him. The timestamp shows it is 11:17 am on Thursday. The man does not appear to be singing or dancing; he is simply standing and smiling. His facial expression suggests he is happy or amused, possibly reacting to the camera or something off-screen. The setting appears to be during the daytime, given the natural light.\\n\\n**Quality**: The video quality is clear, with good visibility and sharpness. The image is well-lit, making the subject easily distinguishable. The camera work is steady, and there is no noticeable shakiness or blurring.\\n\\n**Image Style**: The visual style is naturalistic and casual. The colors are vibrant, with a mix of greens from the trees and earthy tones from the ground and the man's clothing. The overall aesthetic is simple and uncluttered, focusing on the man and his immediate surroundings.\\n\\n**Functiontype & Formtype**: The function of the video is likely to capture a moment of the man's day, perhaps sharing a personal or humorous experience. The form is informal and likely intended for social media platforms, where short clips are common.\\n\\n**Content Restriction Level (restricted)**: The content restriction level is clean and appropriate for all audiences. There are no explicit or inappropriate elements, and the language used is neutral and positive.\",\n" +
                "    \"tags\": {\n" +
                "      \"emotions\": \"joyful\",\n" +
                "      \"imagestyle\": \"vibrant\",\n" +
                "      \"occasion\": \"recreational\",\n" +
                "      \"restricted\": \"clean\",\n" +
                "      \"functiontype\": \"entertainment\",\n" +
                "      \"gender\": \"male\",\n" +
                "      \"quality\": \"high\",\n" +
                "      \"formtype\": \"vlog\",\n" +
                "      \"age\": \"youngadult\",\n" +
                "      \"content\": {\n" +
                "        \"life\": \"dailyvlog\"\n" +
                "      },\n" +
                "      \"scenetype\": \"outdoor\",\n" +
                "      \"appearance\": {\n" +
                "        \"bodytype\": \"average\",\n" +
                "        \"hairstyle\": \"short_hair\",\n" +
                "        \"beauty_level\": \"medium\"\n" +
                "      },\n" +
                "      \"object\": \"human\",\n" +
                "      \"language\": \"english\"\n" +
                "    },\n" +
                "    \"post_id\": 328475200000018750\n" +
                "  },\n" +
                "  \"access_level\": 10,\n" +
                "  \"created_at\": 1761291616,\n" +
                "  \"updated_at\": 1761291616\n" +
                "}";


        try {
            // JsonNode rootNode = objectMapper.readTree(json);
            JsonNode aiPostTagNode = objectMapper.readTree(json);
            //System.out.println("json: \n" + json);

            // 检查event_type
            /*if (!rootNode.has("event_type")) {
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
            }*/

            // 解析post_id和created_at
            //JsonNode postIdNode = aiPostTagNode.path("post_id");
            JsonNode postIdNode = aiPostTagNode.path("post_id");
            if (postIdNode.isMissingNode()) {
                System.out.println("post_id is missing");
                return;
            }
            long postId = postIdNode.asLong();
            if (postId <= 0) {
                System.out.println("post_id is invalid");
                return;
            }

            long createdAt = aiPostTagNode.path("created_at").asLong(0);
            long updatedAt = aiPostTagNode.path("updated_at").asLong(0);
            long duration = System.currentTimeMillis() - updatedAt * 1000;
            System.out.println("createdAt: " + createdAt);
            System.out.println("updatedAt: " + updatedAt);
            System.out.println("duration: " + duration);
            //if (updatedAt <= 0 || duration > durationLimitFromCreatedAt) {
            //    System.out.println("updated_at is invalid");
            //    return;
            //}

            int accessLevel = aiPostTagNode.path("access_level").asInt(0);
            if (accessLevel < accessLevelLow || accessLevel > accessLevelHigh) {
                System.out.println("access_level is invalid");
                return;
            }

            // 解析tags字段
            JsonNode aiTagResultNode = aiPostTagNode.path("results");
            if (aiTagResultNode.isMissingNode()) {
                System.out.println("results is missing");
                return;
            }

            JsonNode aiTagsNode = aiTagResultNode.path("tags");
            if (aiTagsNode.isMissingNode()) {
                System.out.println("tags is missing");
                return;
            }

            // 解析content字段
            JsonNode contentTagNode = aiTagsNode.path("content");
            if (contentTagNode.isMissingNode()) {
                System.out.println("content is missing");
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
                }
            } else if (contentTagNode.isArray()) {
                // 多个 key-value 组成的 list 格式: [{"key1": "value1"}, {"key2": "value2"}]
                for (JsonNode item : contentTagNode) {
                    if (item.isObject()) {
                        Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> field = fields.next();
                            contentTagSet.add(field.getKey());
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
                return;
            }

            // 创建并输出事件
            AiTagParseCommon.PostTagsEvent event = new AiTagParseCommon.PostTagsEvent();
            event.postId = postId;
            event.contentTagsSet = contentTagSet;
            event.createdAt = createdAt;
            event.updatedAt = updatedAt;
            event.accessLevel = accessLevel;

            PrintParseResult(event);
        } catch (Exception e) {
            LOG.error("Failed to parse expose event", e);
        }
        System.out.println("ContentTagJsonParseDemo done.");
    }
}
