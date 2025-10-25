package com.airfree.health.entity;

import lombok.Data;

/**
 * 服务实例健康检查结果类
 */
@Data
public class AirServiceInstanceHealthCheckResult {

    private final boolean healthy;
    private final long timestamp;

    public AirServiceInstanceHealthCheckResult(boolean healthy) {
        this.healthy = healthy;
        this.timestamp = System.currentTimeMillis();
    }
}
