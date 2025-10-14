package com.airfree.log.config;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.logging.LogLevel;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Data
@ConfigurationProperties(prefix = "air-gateway.log")
public class AirGatewayLogProperties {

    // 是否启用日志
    private boolean enabled = true;

    // 日志级别
    private LogLevel level = LogLevel.INFO;

    // 存储类型
    private String storageType = "CONSOLE"; // CONSOLE, FILE, MONGO, ELASTICSEARCH

    // 文件存储路径
    private String logFilePath = "./logs";

    // 是否启用请求日志
    private boolean enableRequestLog = true;

    // 是否启用响应日志
    private boolean enableResponseLog = true;

    // 是否启用访问日志
    private boolean enableAccessLog = true;

    // 是否控制台输出
    private boolean consoleOutput = true;

    // 日志线程池大小
    private int logThreadPoolSize = 10;

    // 日志队列容量
    private int logQueueCapacity = 10000;

    // 需要记录的请求头
    private List<String> logHeaders = Arrays.asList(
            "User-Agent", "Content-Type", "Authorization", "X-.*"
    );

    // 忽略的路径
    private List<String> excludePaths = Arrays.asList(
            "/health", "/actuator/.*"
    );
}


