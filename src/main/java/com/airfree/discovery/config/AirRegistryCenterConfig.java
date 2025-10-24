package com.airfree.discovery.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个注册中心配置
 */
@Data
public class AirRegistryCenterConfig {
    private String type;
    private List<String> serverAddresses;
    private String namespace;
    private String group;
    private String username;
    private String password;
    private String clusterName;
    private Duration connectTimeout = Duration.ofSeconds(3);
    private Duration readTimeout = Duration.ofSeconds(10);
    private boolean enabled = true;

    // 特定注册中心的扩展配置
    private Map<String, Object> extensions = new HashMap<>();
}
