package com.gosh.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import com.gosh.cons.CommonConstants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LogsUtil {
    private static final String DEFAULT_CONFIG_FILE = CommonConstants.LOG_CONFIG;
    private static Properties logProperties;

    static {
        loadLogProperties();
    }

    /**
     * 加载日志配置文件
     */
    private static void loadLogProperties() {
        logProperties = new Properties();
        try (InputStream input = LogsUtil.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            if (input == null) {
                throw new RuntimeException("无法找到日志配置文件: " + DEFAULT_CONFIG_FILE);
            }
            logProperties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("加载日志配置文件失败: " + DEFAULT_CONFIG_FILE, e);
        }
    }

    /**
     * 设置所有日志记录器的级别
     */
    public static void setAllLogLevels() {
        for (String loggerName : logProperties.stringPropertyNames()) {
            String levelStr = logProperties.getProperty(loggerName);
            Level level = Level.toLevel(levelStr, Level.ERROR); // 默认ERROR级别
            Configurator.setLevel(loggerName, level);
        }
    }

    /**
     * 设置指定日志记录器的级别
     * @param loggerName 日志记录器名称
     */
    public static void setLogLevel(String loggerName) {
        String levelStr = logProperties.getProperty(loggerName);
        if (levelStr != null) {
            Level level = Level.toLevel(levelStr, Level.ERROR);
            Configurator.setLevel(loggerName, level);
        }
    }

    /**
     * 获取指定日志记录器的配置级别
     * @param loggerName 日志记录器名称
     * @return 日志级别，如果未配置返回ERROR
     */
    public static Level getLogLevel(String loggerName) {
        String levelStr = logProperties.getProperty(loggerName);
        return Level.toLevel(levelStr, Level.ERROR);
    }
    
    /**
     * 获取指定日志记录器的实际生效级别
     * @param loggerName 日志记录器名称
     * @return 实际生效的日志级别
     */
    public static Level getEffectiveLogLevel(String loggerName) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(loggerName);
        return logger.getLevel();
    }
}