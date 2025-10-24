package com.airfree.discovery.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 同时使用多个注册中心配置
 */
@Data
@ConfigurationProperties(prefix = "discovery.registries")
public class MultiAirRegistryCenterConfig {

    private Map<String, AirRegistryCenterConfig> configs = new HashMap<>();
    private List<String> enabledRegistries = new ArrayList<>();
}
