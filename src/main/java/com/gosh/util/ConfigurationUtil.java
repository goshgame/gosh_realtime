package com.gosh.util;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationUtil {
    /**
     * 从配置文件加载
     *
     * @return Flink Configuration 对象
     */
    public static Configuration loadConfigurationFromProperties(String configFileName) {
        Configuration configuration = new Configuration();

        try (InputStream input = ConfigurationUtil.class.getClassLoader().getResourceAsStream(configFileName)) {
            if (input == null) {
                throw new RuntimeException("无法找到配置文件: " + configFileName);
            }

            Properties props = new Properties();
            props.load(input);
            for (String key : props.stringPropertyNames()) {
                configuration.setString(key, props.getProperty(key));
            }

        } catch (IOException e) {
            throw new RuntimeException("加载配置文件失败: " + configFileName, e);
        }

        return configuration;
    }
}
