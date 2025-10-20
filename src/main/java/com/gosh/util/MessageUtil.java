package com.gosh.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gosh.cons.CommonConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MessageUtil.class);
    private static final ExecutorService NOTIFY_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    // 飞书机器人Webhook地址
    private static String FEISHU_WEBHOOK;
    // 使用ThreadLocal存储作业名称，支持多作业场景
    private static final ThreadLocal<String> JOB_NAME_THREAD_LOCAL = ThreadLocal.withInitial(() -> "default-job");

    static {
        // 加载飞书配置
        Configuration larkConfig = ConfigurationUtil.loadConfigurationFromProperties(CommonConstants.LARK_DEFAULT_CONF);
        FEISHU_WEBHOOK = larkConfig.getString("flink.monitor.lark.webhook", "");
    }
    
    /**
     * 设置当前线程的作业名称
     * @param jobName 作业名称
     */
    public static void setJobName(String jobName) {
        if (jobName != null && !jobName.isEmpty()) {
            JOB_NAME_THREAD_LOCAL.set(jobName);
        } else {
            LOG.warn("尝试设置空的作业名称，将使用默认值");
        }
    }
    
    /**
     * 获取当前线程的作业名称
     * @return 作业名称
     */
    public static String getJobName() {
        return JOB_NAME_THREAD_LOCAL.get();
    }
    
    /**
     * 清理当前线程的作业名称，避免内存泄漏
     */
    public static void removeJobName() {
        JOB_NAME_THREAD_LOCAL.remove();
    }
    /**
     * 发送飞书告警
     */
    public static void sendLarkshuMsg(String message, Throwable throwable) {
        if (FEISHU_WEBHOOK == null || FEISHU_WEBHOOK.isEmpty()) {
            LOG.warn("飞书Webhook未配置，无法发送告警");
            return;
        }

        NOTIFY_EXECUTOR.submit(() -> {
            Map<String, Object> content = new HashMap<>();
            try {
                content.put("text", buildAlertContent(message, throwable));

                Map<String, Object> payload = new HashMap<>();
                payload.put("msg_type", "text");
                payload.put("content", content);

                String jsonPayload = OBJECT_MAPPER.writeValueAsString(payload);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(FEISHU_WEBHOOK))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                        .build();

                HttpResponse<String> response = HTTP_CLIENT.send(
                        request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    LOG.error("发送飞书告警失败，响应码: {}, 响应内容: {}",
                            response.statusCode(), response.body());
                } else {
                    LOG.info("飞书告警发送成功");
                }
            } catch (Exception e) {
                LOG.error("发送飞书告警异常：errorMessage:{} \n,Exception:{}",content,e);
            }
        });
    }

    /**
     * 构建告警内容
     */
    private static String buildAlertContent(String message, Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("【Flink 1.20监控告警】\n"));
        sb.append(String.format("作业名称: %s\n", getJobName()));
        sb.append(String.format("告警信息: %s\n", message));
        if (throwable != null) {
            sb.append("异常堆栈: \n");
            sb.append(getStackTraceAsString(throwable));
        }

        sb.append("运行环境: AWS Flink\n");
        return sb.toString();
    }

    /**
     * 异常堆栈转字符串
     */
    private static String getStackTraceAsString(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : throwable.getStackTrace()) {
            sb.append(element.toString()).append("\n");
            if (sb.length() > 1000) {
                sb.append("...（堆栈信息过长，已截断）");
                break;
            }
        }
        return sb.toString();
    }
}
