package com.airfree.health.enums;

public enum AirHealthCheckStrategyEnum {
    /**
     * 基于注册中心的健康状态
     */
    REGISTRY_BASED,

    /**
     * 客户端主动健康检查
     */
    CLIENT_ACTIVE,

    /**
     * 混合模式（注册中心 + 客户端检查）
     */
    HYBRID,

    /**
     * 宽松模式（即使注册中心标记不健康也返回）
     */
    LENIENT
}
