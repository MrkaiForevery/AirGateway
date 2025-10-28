package com.airfree.discovery.instance;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * serviceInstance实例类包含健康检查逻辑*
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class AirServiceInstance {

    private String instanceId;
    private String serviceId;
    private String host;
    private int port;
    private boolean secure;
    private Map<String, String> metadata;
    private URI uri;

    // 健康状态相关字段
    private volatile boolean registryHealthy = true;  // 注册中心报告的健康状态
    private volatile boolean clientHealthy = true;    // 客户端检查的健康状态
    private volatile long lastHealthCheckTime;
    private volatile boolean inMaintenance = false;

    /**
     * 基础健康状态判断 - 只基于存储的状态
     */
    public boolean isHealthy() {
        // 如果处于维护模式，直接返回不健康
        if (inMaintenance) {
            return false;
        }

        // 默认需要注册中心和客户端都健康
        return registryHealthy && clientHealthy;
    }

    /**
     * 基于元数据的健康状态判断
     */
    public boolean isMetadataHealthy() {
        if (metadata == null) return true;

        // 检查维护标记
        if ("true".equalsIgnoreCase(metadata.get("maintenance")) ||
                "true".equalsIgnoreCase(metadata.get("out_of_service"))) {
            return false;
        }

        // 检查注册中心状态
        String registryStatus = metadata.get("registry.status");
        if (registryStatus != null &&
                !("UP".equalsIgnoreCase(registryStatus) || "HEALTHY".equalsIgnoreCase(registryStatus))) {
            return false;
        }

        return true;
    }

    /**
     * 更新注册中心健康状态
     */
    public void updateRegistryHealth(boolean healthy) {
        this.registryHealthy = healthy;
        this.lastHealthCheckTime = System.currentTimeMillis();
    }

    /**
     * 更新客户端健康状态
     */
    public void updateClientHealth(boolean healthy) {
        this.clientHealthy = healthy;
        this.lastHealthCheckTime = System.currentTimeMillis();
    }

    /**
     * 设置维护模式
     */
    public void setMaintenance(boolean maintenance) {
        this.inMaintenance = maintenance;
    }


    /**
     * 获取服务实例的uri
     */
    public URI getUri() {
        if (uri == null) {
            String scheme = secure ? "https" : "http";
            uri = URI.create(String.format("%s://%s:%d", scheme, host, port));
        }
        return uri;
    }

    /**
     * 获取健康状态摘要
     */
    public Map<String, Object> getHealthSummary() {
        Map<String, Object> summary = new ConcurrentHashMap<>();
        summary.put("overallHealthy", isHealthy());
        summary.put("registryHealthy", registryHealthy);
        summary.put("clientHealthy", clientHealthy);
        summary.put("metadataHealthy", isMetadataHealthy());
        summary.put("inMaintenance", inMaintenance);
        summary.put("lastCheckTime", lastHealthCheckTime);
        return summary;
    }

}
