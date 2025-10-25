package com.airfree.health.entity;

/**
 * 注册中心健康检查实体*
 */
public class AirRegistryCenterHealth {

    private final String registryType;
    private final boolean healthy;
    private final String registryName;
    private final long checkTime;
    private final String errorMessage;

    public AirRegistryCenterHealth(String registryType,
                                   boolean healthy) {
        this(registryType, healthy, "unknown", System.currentTimeMillis(), null);
    }

    public AirRegistryCenterHealth(String registryType,
                                   boolean healthy,
                                   String registryName,
                                   long checkTime,
                                   String errorMessage) {
        this.registryType = registryType;
        this.healthy = healthy;
        this.registryName = registryName;
        this.checkTime = checkTime;
        this.errorMessage = errorMessage;
    }

    public String getRegistryType() {
        return registryType;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public String getRegistryName() {
        return registryName;
    }

    public long getCheckTime() {
        return checkTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return String.format("RegistryHealth{type=%s, healthy=%s, name=%s, checkTime=%d, error=%s}",
                registryType, healthy, registryName, checkTime, errorMessage);
    }
}
