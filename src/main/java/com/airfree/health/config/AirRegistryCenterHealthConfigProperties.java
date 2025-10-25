package com.airfree.health.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "registry-center-health-check")
public class AirRegistryCenterHealthConfigProperties {

    private Duration checkTimeout = Duration.ofSeconds(5);
    private int maxConcurrentChecks = 3;
    private boolean enableDetailedLogging = false;
    private Duration healthCheckInterval = Duration.ofMinutes(1); // 健康检查间隔
    private Duration cacheDuration = Duration.ofMinutes(5); //缓存刷新间隔
}
