package com.airfree.health.core;


import com.airfree.discovery.instance.AirServiceInstance;
import com.airfree.health.config.AirServiceInstanceHealthCheckConfigProperties;
import com.airfree.health.entity.AirServiceInstanceHealthCheckResult;
import com.airfree.health.enums.AirServiceInstanceHealthCheckStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirServiceInstanceHealthCheckService {

    private final AirServiceInstanceHealthCheckConfigProperties config;
    private final Map<String, AirServiceInstanceHealthCheckResult> healthCheckCache = new ConcurrentHashMap<>();


    public AirServiceInstanceHealthCheckService(AirServiceInstanceHealthCheckConfigProperties config) {
        this.config = config;
    }


    // ==================== 检查入口主函数 ====================
    /**
     * 过滤健康实例
     */
    public Flux<AirServiceInstance> filterHealthyInstances(
            Flux<AirServiceInstance> instances,
            String serviceId) {
        return filterHealthyInstances(instances, serviceId, config.getDefaultStrategy());
    }

    /**
     * 根据指定策略过滤健康实例
     */
    public Flux<AirServiceInstance> filterHealthyInstances(Flux<AirServiceInstance> instances,
                                                           String serviceId,
                                                           AirServiceInstanceHealthCheckStrategy strategy) {

        return instances
                .transform(flux -> applyHealthCheckStrategy(flux, serviceId, strategy))
                .switchIfEmpty(handleNoHealthyInstances(serviceId, strategy))
                .doOnNext(instance -> logHealthyInstance(serviceId, instance, strategy))
                .doOnComplete(() -> logHealthCheckSummary(serviceId, strategy));
    }

    /**
     * 应用健康检查策略
     */
    private Flux<AirServiceInstance> applyHealthCheckStrategy(Flux<AirServiceInstance> instances,
                                                              String serviceId,
                                                              AirServiceInstanceHealthCheckStrategy strategy) {

        return instances
                .flatMap(instance -> evaluateInstanceHealth(instance, serviceId, strategy))
                .filter(InstanceHealthResult::isHealthy)
                .map(InstanceHealthResult::getInstance);
    }

    /**
     * 评估实例健康状态
     */
    private Mono<InstanceHealthResult> evaluateInstanceHealth(AirServiceInstance instance,
                                                              String serviceId,
                                                              AirServiceInstanceHealthCheckStrategy strategy) {

        return Mono.fromCallable(() -> {
            boolean isHealthy = false;
            String checkMethod = "";

            switch (strategy) {
                case REGISTRY_BASED:
                    isHealthy = instance.isMetadataHealthy();
                    checkMethod = "REGISTRY_BASED";
                    break;

                case CLIENT_ACTIVE:
                    boolean clientHealth = performClientHealthCheck(instance, serviceId);
                    instance.updateClientHealth(clientHealth);
                    isHealthy = clientHealth;
                    checkMethod = "CLIENT_ACTIVE";
                    break;

                case HYBRID:
                    boolean registryHealth = instance.isMetadataHealthy();
                    boolean activeHealth = performClientHealthCheck(instance, serviceId);
                    instance.updateClientHealth(activeHealth);
                    isHealthy = registryHealth && activeHealth;
                    checkMethod = "HYBRID";
                    break;

                case LENIENT:
                    boolean lenientRegistryHealth = instance.isMetadataHealthy();
                    boolean lenientActiveHealth = performClientHealthCheck(instance, serviceId);
                    boolean recoverable = isInstanceRecoverable(instance);
                    instance.updateClientHealth(lenientActiveHealth);
                    isHealthy = lenientRegistryHealth || lenientActiveHealth || recoverable;
                    checkMethod = "LENIENT";
                    break;

                default:
                    isHealthy = instance.isMetadataHealthy();
                    checkMethod = "DEFAULT";
            }

            log.debug("健康检查结果: {} -> {} (策略: {})",
                    instance.getInstanceId(), isHealthy, checkMethod);

            return new InstanceHealthResult(instance, isHealthy, checkMethod);
        });
    }


    // ==================== 客户端健康检查方法 ====================

    /**
     * 执行客户端主动健康检查
     */
    private boolean performClientHealthCheck(AirServiceInstance instance, String serviceId) {
        String cacheKey = instance.getInstanceId();
        AirServiceInstanceHealthCheckResult cachedResult = healthCheckCache.get(cacheKey);

        // 检查缓存
        if (cachedResult != null && !isCacheExpired(cachedResult.getTimestamp())) {
            return cachedResult.isHealthy();
        }

        boolean isHealthy = doClientHealthCheck(instance, serviceId);

        // 更新缓存
        healthCheckCache.put(cacheKey, new AirServiceInstanceHealthCheckResult(isHealthy));

        return isHealthy;
    }


    /**
     * 执行实际的客户端健康检查
     */
    private boolean doClientHealthCheck(AirServiceInstance instance, String serviceId) {
        String host = instance.getHost();
        int port = config.getClientCheckPort() > 0 ? config.getClientCheckPort() : instance.getPort();

        try {
            boolean reachable = isReachable(host, port, config.getClientCheckTimeout());
            log.debug("TCP健康检查: {}:{} -> {}", host, port, reachable);
            return reachable;
        } catch (Exception e) {
            log.debug("客户端健康检查失败: {} -> {}:{}",
                    serviceId, host, port, e);
            return false;
        }
    }

    /**
     * 检查主机端口是否可达
     */
    private boolean isReachable(String host, int port, Duration timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), (int) timeout.toMillis());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 检查实例是否可恢复（用于宽松模式）
     */
    private boolean isInstanceRecoverable(AirServiceInstance instance) {
        Map<String, String> metadata = instance.getMetadata();

        // 如果实例有"maintenance"标记，即使不健康也不返回
        if ("true".equals(metadata.get("maintenance"))) {
            return false;
        }

        // 如果实例有"forceHealthy"标记，强制返回
        if ("true".equals(metadata.get("forceHealthy"))) {
            return true;
        }

        // 检查实例的最后不健康时间
        String lastUnhealthyTime = metadata.get("lastUnhealthyTime");
        if (lastUnhealthyTime != null) {
            try {
                long unhealthyTime = Long.parseLong(lastUnhealthyTime);
                long currentTime = System.currentTimeMillis();
                long maxUnhealthyMillis = config.getLenientMaxUnhealthyMinutes() * 60 * 1000;

                if ((currentTime - unhealthyTime) > maxUnhealthyMillis) {
                    return false; // 超过最大不健康时长
                }
            } catch (NumberFormatException e) {
                log.debug("解析最后不健康时间失败: {}", instance.getInstanceId());
            }
        }

        // 检查实例的权重，低权重实例在不健康时不被考虑
        String weightStr = metadata.get("weight");
        if (weightStr != null) {
            try {
                int weight = Integer.parseInt(weightStr);
                return weight > 50; // 只有权重较高的实例在宽松模式下被考虑
            } catch (NumberFormatException e) {
                log.debug("解析权重失败: {}", instance.getInstanceId());
            }
        }

        return false;
    }


    // ==================== 降级处理 ====================

    /**
     * 处理没有健康实例的情况
     */
    private Flux<AirServiceInstance> handleNoHealthyInstances(String serviceId,
                                                              AirServiceInstanceHealthCheckStrategy strategy) {

        return Mono.defer(() -> {
            log.warn("没有找到健康的服务实例: {}, 策略: {}", serviceId, strategy);

            if (!config.isEnableFallback()) {
                return Mono.empty();
            }

            // 返回降级实例
            return getFallbackInstances(serviceId, strategy)
                    .doOnNext(instances ->
                            log.warn("使用降级实例: {}, 数量: {}", serviceId, instances.size()));
        }).flatMapMany(Flux::fromIterable);
    }


    /**
     * 获取降级实例
     */
    private Mono<List<AirServiceInstance>> getFallbackInstances(String serviceId,
                                                                AirServiceInstanceHealthCheckStrategy strategy) {

        // 这里需要从原始数据源获取实例，实际实现中需要注入相应的依赖
        // 暂时返回空列表，具体实现需要根据实际情况调整
        return Mono.just(new ArrayList<>());
    }


    // ==================== 缓存管理 ====================

    /**
     * 检查缓存是否过期
     */
    private boolean isCacheExpired(long timestamp) {
        return (System.currentTimeMillis() - timestamp) > config.getCacheDuration().toMillis();
    }

    /**
     * 清理健康检查缓存
     */
    public void clearCache() {
        healthCheckCache.clear();
        log.debug("健康检查缓存已清理");
    }

    /**
     * 清理指定实例的健康检查缓存
     */
    public void clearCacheForInstance(String instanceId) {
        healthCheckCache.remove(instanceId);
        log.debug("实例健康检查缓存已清理: {}", instanceId);
    }

    /**
     * 获取健康检查统计信息
     */
    public Map<String, Object> getHealthCheckStats() {
        long healthyCount = healthCheckCache.values().stream()
                .filter(AirServiceInstanceHealthCheckResult::isHealthy)
                .count();
        long totalCount = healthCheckCache.size();
        double healthRatio = totalCount > 0 ? (double) healthyCount / totalCount : 0.0;
        Map<String,Object> resultMap = new HashMap<>();
        resultMap.put("cacheSize", totalCount);
        resultMap.put("healthyCount", healthyCount);
        resultMap.put("healthRatio", healthRatio);
        resultMap.put("config", config);
        resultMap.put("timestamp", System.currentTimeMillis());

        return resultMap;
    }

    // ==================== 内部类 ====================

    /**
     * 内部健康检查结果类
     */
    private static class InstanceHealthResult {
        private final AirServiceInstance instance;
        private final boolean healthy;
        private final String checkMethod;

        public InstanceHealthResult(AirServiceInstance instance, boolean healthy, String checkMethod) {
            this.instance = instance;
            this.healthy = healthy;
            this.checkMethod = checkMethod;
        }

        public AirServiceInstance getInstance() {
            return instance;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public String getCheckMethod() {
            return checkMethod;
        }
    }

    // ==================== 私有日志方法 ====================

    private void logHealthyInstance(String serviceId, AirServiceInstance instance,
                                    AirServiceInstanceHealthCheckStrategy strategy) {
        if (log.isDebugEnabled()) {
            Map<String, String> metadata = instance.getMetadata();
            String version = metadata.get("version");
            String zone = metadata.get("zone");

            log.debug("发现健康实例: {} -> {}:{} (版本: {}, 区域: {}, 策略: {})",
                    serviceId, instance.getHost(), instance.getPort(),
                    version, zone, strategy);
        }
    }

    private void logHealthCheckSummary(String serviceId, AirServiceInstanceHealthCheckStrategy strategy) {
        log.debug("健康检查完成: {}, 策略: {}", serviceId, strategy);
    }
}
