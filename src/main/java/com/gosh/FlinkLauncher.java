package com.gosh;

//import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkLauncher.class);
    private static final String CLASS_NAME_PARAM = "className";
    private static final String JOB_PACKAGE_PREFIX = "com.gosh.job.";

    public static void main(String[] args) throws Exception {
        // 1. 解析命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 2. 获取要执行的Job类名（优先从命令行参数获取）
        String className = parameterTool.get(CLASS_NAME_PARAM);

        // 3. 如果命令行未指定，从AWS Kinesis配置中获取
//        if (className == null || className.trim().isEmpty()) {
//            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
//            Properties consumerProperties = applicationProperties.get("ConsumerConfigProperties");
//            LOG.info("从AWS获取的ConsumerConfigProperties: {}", consumerProperties);
//
//            if (consumerProperties != null) {
//                className = consumerProperties.getProperty(CLASS_NAME_PARAM);
//            }
//        }

        // 4. 验证类名
        if (className == null || className.trim().isEmpty()) {
            throw new IllegalArgumentException("未找到className参数，请通过--className命令行参数或AWS配置指定");
        }

        // 5. 确保类名安全（限制在job包下，防止恶意类加载）
        String fullClassName = ensureSafeClassName(className);

        // 6. 准备传递给Job的参数（合并命令行参数和AWS配置）
        String[] jobArgs = prepareJobArguments(parameterTool);

        // 7. 通过反射执行目标Job类的main方法
        executeJobClass(fullClassName, jobArgs);
    }

    /**
     * 确保类名安全，限制在指定的job包下
     */
    private static String ensureSafeClassName(String className) {
        // 如果已经是全限定名且在指定包下，直接使用
        if (className.startsWith(JOB_PACKAGE_PREFIX)) {
            return className;
        }
        // 否则拼接包前缀
        return JOB_PACKAGE_PREFIX + className;
    }

    /**
     * 准备传递给Job的参数，将ParameterTool中的参数转换为--key=value格式
     */
    private static String[] prepareJobArguments(ParameterTool parameterTool) {
        Map<String, String> params = new HashMap<>(parameterTool.toMap());
        // 移除className参数，避免传递给Job
        params.remove(CLASS_NAME_PARAM);

        // 转换为命令行参数格式
        return params.entrySet().stream()
                .map(entry -> String.format("--%s=%s", entry.getKey(), entry.getValue()))
                .toArray(String[]::new);
    }

    /**
     * 通过反射执行目标Job类的main方法
     */
    private static void executeJobClass(String className, String[] args) throws Exception {
        Class<?> jobClass = Class.forName(className);
        Method mainMethod = jobClass.getMethod("main", String[].class);

        // 调用main方法执行Job
        mainMethod.invoke(null, (Object) args);
    }

}
