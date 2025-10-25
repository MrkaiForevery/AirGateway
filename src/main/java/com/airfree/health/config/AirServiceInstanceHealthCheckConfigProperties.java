package com.airfree.health.config;

import com.airfree.health.enums.AirServiceInstanceHealthCheckStrategy;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties(prefix = "service-instance-health-check")
public class AirServiceInstanceHealthCheckConfigProperties {

    /**
     * 默认健康检查策略
     */
    private AirServiceInstanceHealthCheckStrategy defaultStrategy = AirServiceInstanceHealthCheckStrategy.REGISTRY_BASED;

    /**
     * 客户端健康检查超时时间
     */
    private Duration clientCheckTimeout = Duration.ofSeconds(3);

    /**
     * 客户端健康检查端口
     */
    private int clientCheckPort = -1; // -1 表示使用实例端口

    /**
     * 宽松模式下考虑的实例最大不健康时长（分钟）
     */
    private int lenientMaxUnhealthyMinutes = 10;

    /**
     * 是否启用降级实例
     */
    private boolean enableFallback = true;

    /**
     * 降级实例最大返回数量
     */
    private int maxFallbackInstances = 1;

    /**
     * 健康检查缓存时间
     */
    private Duration cacheDuration = Duration.ofSeconds(30);

}
