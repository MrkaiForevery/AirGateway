package com.airfree.health.core;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.health.config.AirRegistryCenterHealthConfigProperties;
import com.airfree.health.entity.AirRegistryCenterHealth;
import com.airfree.health.entity.AirRegistryCenterHealthStats;
import com.airfree.health.event.AirRegistryCenterHealthEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirRegistryCenterHealthService {

    private final AirRegistryCenterHealthConfigProperties config;
    private final Map<String, AirRegistryCenterHealth> healthCache = new ConcurrentHashMap<>();

    public AirRegistryCenterHealthService(AirRegistryCenterHealthConfigProperties config) {
        this.config = config;
    }

    // ==================== 核心健康检查方法 ====================

    /**
     * 检查单个注册中心客户端的健康状态
     */
    public Mono<AirRegistryCenterHealth> checkRegistryHealth(AirReactiveServiceDiscovery discovery) {
        return checkRegistryHealth(discovery, "unknown");
    }

    /**
     * 检查单个注册中心客户端的健康状态（带名称）
     */
    public Mono<AirRegistryCenterHealth> checkRegistryHealth(AirReactiveServiceDiscovery discovery, String registryName) {
        String registryType = discovery.getRegistryType();
        String cacheKey = registryName + ":" + registryType;

        // 检查缓存（可选，根据需求决定是否使用缓存）
        AirRegistryCenterHealth cachedHealth = healthCache.get(cacheKey);
        if (cachedHealth != null && ! isCacheExpired(cachedHealth.getCheckTime())) {
            return Mono.just(cachedHealth);
        }

        return discovery.isHealthy()
                .timeout(config.getCheckTimeout())
                .map(healthy -> {
                    AirRegistryCenterHealth health = new AirRegistryCenterHealth(
                            registryType, healthy, registryName,
                            System.currentTimeMillis(), null
                    );
                    // 缓存健康状态
                    healthCache.put(cacheKey, health);

                    if (config.isEnableDetailedLogging()) {
                        log.debug("注册中心健康检查成功: {} - {}, 健康状态: {}", registryName, registryType, healthy);
                    }
                    return health;
                })
                .onErrorResume(error -> {
                    AirRegistryCenterHealth health = new AirRegistryCenterHealth(
                            registryType, false, registryName,
                            System.currentTimeMillis(), error.getMessage()
                    );
                    // 缓存不健康状态
                    healthCache.put(cacheKey, health);

                    log.warn("注册中心健康检查失败: {} - {}", registryName, registryType, error);
                    return Mono.just(health);
                })
                .doOnSubscribe(subscription -> {
                    if (config.isEnableDetailedLogging()) {
                        log.debug("开始注册中心健康检查: {} - {}", registryName, registryType);
                    }
                });
    }


    /**
     * 批量检查多个注册中心的健康状态
     */
    public Flux<AirRegistryCenterHealth> checkMultipleRegistries(List<AirReactiveServiceDiscovery> discoveries) {
        return checkMultipleRegistries(discoveries, discovery -> "unknown");
    }

    /**
     * 批量检查多个注册中心的健康状态（带名称映射）
     */
    public Flux<AirRegistryCenterHealth> checkMultipleRegistries(
            List<AirReactiveServiceDiscovery> discoveries,
            java.util.function.Function<AirReactiveServiceDiscovery, String> nameMapper) {

        return Flux.fromIterable(discoveries)
                .flatMap(discovery -> {
                    String registryName = nameMapper.apply(discovery);
                    return checkRegistryHealth(discovery, registryName);
                }, config.getMaxConcurrentChecks()) // 控制并发度
                .sort((h1, h2) -> Boolean.compare(h2.isHealthy(), h1.isHealthy())); // 健康的状态排在前面
    }

    /**
     * 获取注册中心健康状态统计
     */
    public Mono<AirRegistryCenterHealthStats> getRegistryHealthStats(List<AirReactiveServiceDiscovery> discoveries) {
        return getRegistryHealthStats(discoveries, discovery -> "unknown");
    }

    /**
     * 获取注册中心健康状态统计（带名称映射）
     */
    public Mono<AirRegistryCenterHealthStats> getRegistryHealthStats(
            List<AirReactiveServiceDiscovery> discoveries,
            java.util.function.Function<AirReactiveServiceDiscovery, String> nameMapper) {

        return checkMultipleRegistries(discoveries, nameMapper)
                .collectList()
                .map(this::calculateHealthStats);
    }

    /**
     * 获取总体健康状态（所有注册中心是否都健康）
     */
    public Mono<Boolean> getOverallHealthStatus(List<AirReactiveServiceDiscovery> discoveries) {
        return checkMultipleRegistries(discoveries)
                .all(AirRegistryCenterHealth::isHealthy)
                .onErrorReturn(false); // 如果检查过程中出错，认为不健康
    }

    // ==================== 健康检查事件发布 ====================

    /**
     * 订阅注册中心健康状态变化
     */
    public Flux<AirRegistryCenterHealthEvent> subscribeHealthChanges(List<AirReactiveServiceDiscovery> discoveries) {
        return Flux.interval(config.getHealthCheckInterval())
                .flatMap(tick -> checkMultipleRegistries(discoveries))
                .distinctUntilChanged(health ->
                        health.isHealthy() + ":" + health.getRegistryName() + ":" + health.getErrorMessage()
                )
                .map(health -> new AirRegistryCenterHealthEvent(health, System.currentTimeMillis()))
                .onErrorContinue((error, value) -> {
                    log.warn("健康检查订阅发生错误，继续执行", error);
                });
    }

    // ==================== 缓存管理 ====================

    /**
     * 清理健康检查缓存
     */
    public void clearCache() {
        healthCache.clear();
        log.debug("注册中心健康检查缓存已清理");
    }

    /**
     * 清理指定注册中心的健康检查缓存
     */
    public void clearCacheForRegistry(String registryName, String registryType) {
        String cacheKey = registryName + ":" + registryType;
        healthCache.remove(cacheKey);
        log.debug("注册中心健康检查缓存已清理: {}", cacheKey);
    }

    /**
     * 获取缓存统计信息
     */
    public Map<String, Object> getCacheStats() {
        long healthyCount = healthCache.values().stream().filter(AirRegistryCenterHealth::isHealthy).count();
        long totalCount = healthCache.size();
        double healthRatio = totalCount > 0 ? (double) healthyCount / totalCount : 0.0;
        Map<String,Object> resultMap = new HashMap<>();
        resultMap.put("cacheSize", totalCount);
        resultMap.put("healthyCount", healthyCount);
        resultMap.put("healthRatio", healthRatio);
        resultMap.put("timestamp", System.currentTimeMillis());
        return resultMap;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 计算健康状态统计
     */
    private AirRegistryCenterHealthStats calculateHealthStats(List<AirRegistryCenterHealth> healthList) {
        int total = healthList.size();
        long healthyCount = healthList.stream().filter(AirRegistryCenterHealth::isHealthy).count();
        double healthRatio = total > 0 ? (double) healthyCount / total : 0.0;

        return new AirRegistryCenterHealthStats(total, healthyCount, healthRatio, healthList);
    }

    /**
     * 检查缓存是否过期（5分钟过期）
     */
    private boolean isCacheExpired(long timestamp) {
        return (System.currentTimeMillis() - timestamp) > Duration.ofMinutes(5).toMillis();
    }

    /**
     * 获取配置
     */
    public AirRegistryCenterHealthConfigProperties getConfig() {
        return config;
    }

}
