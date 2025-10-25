package com.airfree.discovery.core;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.cache.AirServiceDiscoveryCache;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.config.MultiAirRegistryCenterConfig;
import com.airfree.discovery.event.AirServiceDiscoveryEvent;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import com.airfree.health.core.AirRegistryCenterHealthService;
import com.airfree.health.core.AirServiceInstanceHealthCheckService;
import com.airfree.health.entity.AirRegistryCenterHealth;
import com.airfree.health.entity.AirRegistryCenterHealthStats;
import com.airfree.health.enums.AirServiceInstanceHealthCheckStrategy;
import com.airfree.health.event.AirRegistryCenterHealthEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirReactiveServiceDiscoveryManager {

    //============= 依赖注入 =================
    private final AirReactiveRegistryCenterFactory registryFactory;
    private final MultiAirRegistryCenterConfig multiRegistryConfig;
    private final AirServiceDiscoveryCache cache;
    private final AirServiceInstanceHealthCheckService serviceInstanceHealthCheckService;
    private final AirRegistryCenterHealthService registryCenterHealthService;
    //todo 这个事件发布器，统一编写实现类listener进行搞，要规整，还要在构造方法里面初始化
    private final ApplicationEventPublisher eventPublisher;

    //============= 内部状态指标 =================
    private final Map<String, List<AirReactiveServiceDiscovery>> serviceDiscoveries = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;
    private final Mono<Void> initializationMono;

    //============= 性能指标 ====================
    private final Counter discoveryCounter;
    private final Timer discoveryTimer;


    //============= 构造方法 =================
    public AirReactiveServiceDiscoveryManager(AirReactiveRegistryCenterFactory registryFactory,
                                              MultiAirRegistryCenterConfig multiRegistryConfig,
                                              AirServiceInstanceHealthCheckService serviceInstanceHealthCheckService,
                                              AirRegistryCenterHealthService registryCenterHealthService,
                                              ApplicationEventPublisher eventPublisher) {
        this.registryFactory = registryFactory;
        this.multiRegistryConfig = multiRegistryConfig;
        this.serviceInstanceHealthCheckService = serviceInstanceHealthCheckService;
        this.registryCenterHealthService = registryCenterHealthService;
        this.eventPublisher = eventPublisher;
        //todo 这里先写死构造方法的两个参数值，以后再从外部读取
        this.cache = new AirServiceDiscoveryCache(30, 1000);
        //todo 这里异步初始化，需要有个标志句柄表示初始化是否完全完成
        initializationMono = initialize().cache();
        // 初始化监控指标
        this.discoveryCounter = Metrics.counter("service.discovery.requests");
        this.discoveryTimer = Metrics.timer("service.discovery.duration");
    }

    // ==================== 初始化相关 ====================

    /**
     * 初始化所有注册中心
     */
    public Mono<Void> initialize() {
        log.info("开始初始化服务发现管理器...");
        return validateConfig()
                .then(Flux.fromIterable(multiRegistryConfig.getConfigs().entrySet())
                        .filter(entry -> entry.getValue().isEnabled())
                        .flatMap(this::initializeRegistryDiscovery)
                        .then()
                        .doOnSuccess(v -> {
                            initialized = true;
                            int registryCount = serviceDiscoveries.values().stream().mapToInt(List::size).sum();
                            log.info("服务发现管理器初始化完成，注册中心数量: {}", registryCount);
                            // 发布初始化完成事件
                            publishManagerInitializedEvent(registryCount);
                        })
                        .doOnError(error -> log.error("服务发现管理器初始化失败", error)));
    }

    /**
     * 初始化单个注册中心发现客户端
     */
    private Mono<AirReactiveServiceDiscovery> initializeRegistryDiscovery(Map.Entry<String, AirRegistryCenterConfig> entry) {
        String name = entry.getKey();
        AirRegistryCenterConfig config = entry.getValue();

        return registryFactory
                .createDiscovery(config)
                .doOnSuccess(discovery -> registerDiscovery(config.getType(), discovery, name))
                .onErrorResume(e -> handleDiscoveryInitError(name, config.getType(), e));
    }

    /**
     * 注册发现客户端
     */
    private void registerDiscovery(String type, AirReactiveServiceDiscovery discovery, String name) {
        serviceDiscoveries
                .computeIfAbsent(type, k -> new ArrayList<>())
                .add(discovery);
        log.info("注册中心客户端初始化成功: {} - {}", name, type);
        // 发布注册中心注册事件
        publishRegistryRegisteredEvent(name, type, discovery);
    }

    /**
     * 处理发现客户端初始化错误
     */
    private Mono<AirReactiveServiceDiscovery> handleDiscoveryInitError(String name, String type, Throwable error) {
        log.error("注册中心客户端初始化失败: {} - {}", name, type, error);
        publishRegistryInitFailedEvent(name, type, error);
        return Mono.empty();
    }


    // ==================== 服务发现核心方法 ====================

    /**
     * 从所有注册中心发现服务实例（带缓存）
     */
    public Flux<AirServiceInstance> discoverService(String serviceId) {
        return ensureInitialized()
                .then(Mono.fromCallable(() -> discoveryTimer.record(() -> {
                    discoveryCounter.increment();
                    return cache.getOrLoad(serviceId, this::fetchInstancesFromAllRegistries);
                })))
                .flatMapMany(Function.identity());
    }

    /**
     * 从所有注册中心获取服务实例（无缓存）
     */
    private Flux<AirServiceInstance> fetchInstancesFromAllRegistries(String serviceId) {
        return Flux.fromIterable(serviceDiscoveries.values())
                .flatMap(Flux::fromIterable)
                .flatMap(discovery -> fetchInstancesFromDiscovery(discovery, serviceId))
                .distinct(AirServiceInstance::getInstanceId)
                .collectList()
                .doOnSuccess(instances -> publishDiscoveryEvent(serviceId, instances))
                .flatMapMany(Flux::fromIterable);
    }

    /**
     * 从单个发现客户端获取实例
     */
    private Flux<AirServiceInstance> fetchInstancesFromDiscovery(AirReactiveServiceDiscovery discovery,
                                                                 String serviceId) {
        return discovery.getInstances(serviceId)
                .onErrorResume(e -> handleDiscoveryError(discovery, serviceId, e));
    }

    /**
     * 处理单个发现客户端的错误
     */
    private Flux<AirServiceInstance> handleDiscoveryError(AirReactiveServiceDiscovery discovery,
                                                          String serviceId, Throwable error) {
        log.warn("从 {} 发现服务 {} 失败", discovery.getRegistryType(), serviceId, error);
        publishDiscoveryErrorEvent(discovery, serviceId, error);
        return Flux.empty();
    }

    // ==================== 服务实例健康检查集成 ====================

    /**
     * 获取健康的服务实例（使用默认策略）
     */
    public Flux<AirServiceInstance> getHealthyInstances(String serviceId) {
        return discoverService(serviceId)
                .transform(flux -> serviceInstanceHealthCheckService.filterHealthyInstances(flux, serviceId));
    }

    /**
     * 根据指定策略获取健康实例
     */
    public Flux<AirServiceInstance> getHealthyInstances(String serviceId,
                                                        AirServiceInstanceHealthCheckStrategy strategy) {
        return discoverService(serviceId)
                .transform(flux -> serviceInstanceHealthCheckService.filterHealthyInstances(flux, serviceId, strategy));
    }

    /**
     * 获取可用的服务实例（忽略注册中心状态，只检查连通性）
     */
    public Flux<AirServiceInstance> getAvailableInstances(String serviceId) {
        return getHealthyInstances(serviceId, AirServiceInstanceHealthCheckStrategy.CLIENT_ACTIVE);
    }

    /**
     * 获取宽松模式的健康实例
     */
    public Flux<AirServiceInstance> getLenientHealthyInstances(String serviceId) {
        return getHealthyInstances(serviceId, AirServiceInstanceHealthCheckStrategy.LENIENT);
    }

    /**
     * 获取混合模式检查的健康实例
     */
    public Flux<AirServiceInstance> getHybridHealthyInstances(String serviceId) {
        return getHealthyInstances(serviceId, AirServiceInstanceHealthCheckStrategy.HYBRID);
    }


    // ==================== 服务实例查询与订阅 ====================

    /**
     * 获取所有可用的服务名称
     */
    public Flux<String> getAllServiceNames() {
        return ensureInitialized()
                .thenMany(Flux.fromIterable(serviceDiscoveries.values())
                        .flatMap(Flux::fromIterable)
                        .flatMap(AirReactiveServiceDiscovery::getServiceNames)
                        .distinct());
    }

    /**
     * 订阅服务变化
     */
    public Flux<AirServiceInstanceEvent> subscribeService(String serviceId) {
        return ensureInitialized()
                .thenMany(Flux.fromIterable(serviceDiscoveries.values())
                        .flatMap(Flux::fromIterable)
                        .flatMap(discovery -> discovery.subscribe(serviceId))
                        .onErrorResume(this::handleSubscriptionError));
    }


    /**
     * 根据元数据过滤服务实例
     */
    public Flux<AirServiceInstance> getInstancesWithMetadata(String serviceId,
                                                             Map<String, String> metadata) {
        return discoverService(serviceId)
                .filter(instance -> hasMetadata(instance, metadata));
    }

    /**
     * 根据谓词过滤服务实例
     */
    public Flux<AirServiceInstance> getInstancesWithPredicate(String serviceId,
                                                              Predicate<AirServiceInstance> predicate) {
        return discoverService(serviceId).filter(predicate);
    }

    /**
     * 根据版本过滤服务实例
     */
    public Flux<AirServiceInstance> getInstancesByVersion(String serviceId, String version) {
        return getInstancesWithMetadata(serviceId, Map.of("version", version));
    }

    /**
     * 根据区域过滤服务实例
     */
    public Flux<AirServiceInstance> getInstancesByZone(String serviceId, String zone) {
        return getInstancesWithMetadata(serviceId, Map.of("zone", zone));
    }

    // ==================== 注册中心健康检查集成 ====================

    /**
     * 获取所有注册中心的健康状态
     */
    public Flux<AirRegistryCenterHealth> getRegistryHealth() {
        return ensureInitialized()
                .thenMany(getAllDiscoveriesAsFlux())
                .flatMap(discovery ->
                                registryCenterHealthService.checkRegistryHealth(discovery, getDiscoveryName(discovery)),
                        3 // 控制并发度
                )
                .sort((h1, h2) -> {
                    // 按健康状态排序：健康的在前
                    if (h1.isHealthy() != h2.isHealthy()) {
                        return Boolean.compare(h2.isHealthy(), h1.isHealthy());
                    }
                    // 然后按类型排序
                    return h1.getRegistryType().compareTo(h2.getRegistryType());
                });
    }

    /**
     * 获取注册中心健康统计
     */
    public Mono<AirRegistryCenterHealthStats> getRegistryHealthStats() {
        return ensureInitialized()
                .then(getAllDiscoveriesAsFlux().collectList())
                .flatMap(discoveries ->
                        registryCenterHealthService.getRegistryHealthStats(discoveries, this::getDiscoveryName)
                );
    }

    /**
     * 获取总体健康状态（所有注册中心是否都健康）
     */
    public Mono<Boolean> getOverallRegistryHealth() {
        return ensureInitialized()
                .then(getAllDiscoveriesAsFlux().collectList())
                .flatMap(registryCenterHealthService::getOverallHealthStatus);
    }

    /**
     * 订阅注册中心健康状态变化
     */
    public Flux<AirRegistryCenterHealthEvent> subscribeRegistryHealthChanges() {
        return ensureInitialized()
                .then(getAllDiscoveriesAsFlux().collectList())
                .flatMapMany(discoveries ->
                        registryCenterHealthService.subscribeHealthChanges(discoveries)
                );
    }

    /**
     * 按类型获取注册中心健康状态
     */
    public Flux<RegistryHealthByType> getRegistryHealthByType() {
        return ensureInitialized()
                .thenMany(Flux.fromIterable(serviceDiscoveries.entrySet()))
                .flatMap(entry -> {
                    String registryType = entry.getKey();
                    List<AirReactiveServiceDiscovery> discoveries = entry.getValue();

                    return registryCenterHealthService.checkMultipleRegistries(discoveries)
                            .collectList()
                            .map(healthList -> new RegistryHealthByType(registryType, healthList));
                });
    }


    // ==================== 缓存管理集成 ====================

    /**
     * 强制刷新缓存（立即刷新）
     */
    public Mono<Void> refreshCache(String serviceId) {
        log.info("开始刷新服务缓存: {}", serviceId);

        return ensureInitialized()
                .then(Mono.fromRunnable(() -> cache.invalidateCache(serviceId)))
                .then(discoverService(serviceId).collectList())
                .doOnSuccess(instances -> {
                    logRefreshSuccess(serviceId, instances.size());
                    publishCacheRefreshEvent(serviceId, instances.size(), true, null);
                })
                .doOnError(error -> {
                    logRefreshError(serviceId, error);
                    publishCacheRefreshEvent(serviceId, 0, false, error);
                })
                .then();
    }


    /**
     * 带重试机制的缓存刷新
     */
    public Mono<Void> refreshCacheWithRetry(String serviceId) {
        return refreshCacheWithRetry(serviceId, 3, Duration.ofSeconds(1));
    }

    /**
     * 带重试机制的缓存刷新
     */
    public Mono<Void> refreshCacheWithRetry(String serviceId, int maxAttempts, Duration delay) {
        log.info("开始带重试的缓存刷新: {}, 最大尝试次数: {}", serviceId, maxAttempts);

        return ensureInitialized()
                .then(Mono.fromRunnable(() -> cache.invalidateCache(serviceId)))
                .then(Mono.defer(() -> discoverService(serviceId).collectList()))
                .retryWhen(buildRetryStrategy(serviceId, maxAttempts, delay))
                .doOnSuccess(instances -> {
                    logRefreshSuccess(serviceId, instances.size());
                    publishCacheRefreshEvent(serviceId, instances.size(), true, null);
                })
                .doOnError(error -> {
                    logRefreshError(serviceId, error);
                    publishCacheRefreshEvent(serviceId, 0, false, error);
                })
                .then();
    }

    /**
     * 批量刷新多个服务的缓存
     */
    public Mono<Void> refreshMultipleCaches(List<String> serviceIds) {
        log.info("批量刷新服务缓存: {}", serviceIds);

        return ensureInitialized()
                .then(Flux.fromIterable(serviceIds)
                        .flatMap(this::refreshSingleCache, 3) // 并发度为3
                        .then())
                .doOnSuccess(v -> {
                    log.info("批量刷新完成，共 {} 个服务", serviceIds.size());
                    publishBatchCacheRefreshEvent(serviceIds, true, null);
                })
                .doOnError(error -> {
                    log.error("批量刷新过程中发生错误", error);
                    publishBatchCacheRefreshEvent(serviceIds, false, error);
                });
    }

    /**
     * 缓存定期刷新机制
     */
    public Mono<Void> scheduleCacheRefresh(String serviceId, Duration interval) {
        return Flux.interval(interval)
                .flatMap(tick -> refreshCache(serviceId))
                .then();
    }

    /**
     * 获取缓存统计信息
     */
    public Mono<Map<String, Object>> getCacheStats() {
        return Mono.fromCallable(cache::getStats);
    }

    // ==================== 健康检查缓存管理 ====================

    /**
     * 清理服务实例健康检查缓存
     */
    public Mono<Void> clearInstanceHealthCache() {
        return Mono.fromRunnable(() -> {
            serviceInstanceHealthCheckService.clearCache();
            log.info("服务实例健康检查缓存已清理");
        });
    }

    /**
     * 清理注册中心健康检查缓存
     */
    public Mono<Void> clearRegistryHealthCache() {
        return Mono.fromRunnable(() -> {
            registryCenterHealthService.clearCache();
            log.info("注册中心健康检查缓存已清理");
        });
    }

    /**
     * 清理所有健康检查缓存
     */
    public Mono<Void> clearAllHealthCaches() {
        return Mono.fromRunnable(() -> {
            serviceInstanceHealthCheckService.clearCache();
            registryCenterHealthService.clearCache();
            log.info("所有健康检查缓存已清理");
        });
    }

    /**
     * 获取服务实例健康检查统计信息
     */
    public Mono<Map<String, Object>> getInstanceHealthStats() {
        return Mono.fromCallable(serviceInstanceHealthCheckService::getHealthCheckStats);
    }

    /**
     * 获取注册中心健康检查统计信息
     */
    public Mono<Map<String, Object>> getRegistryHealthCacheStats() {
        return Mono.fromCallable(registryCenterHealthService::getCacheStats);
    }


    // ==================== 私有辅助方法 ====================

    /**
     * 确保服务已初始化
     */
    private Mono<Void> ensureInitialized() {
        return initializationMono;
    }

    /**
     * 获取所有发现客户端作为Flux（展平Map结构）
     */
    private Flux<AirReactiveServiceDiscovery> getAllDiscoveriesAsFlux() {
        return Flux.fromIterable(serviceDiscoveries.values())
                .flatMap(Flux::fromIterable);
    }

    /**
     * 获取发现客户端的显示名称
     */
    private String getDiscoveryName(AirReactiveServiceDiscovery discovery) {
        String registryType = discovery.getRegistryType();

        // 查找该类型下的所有发现客户端
        List<AirReactiveServiceDiscovery> discoveriesOfType = serviceDiscoveries.get(registryType);
        if (discoveriesOfType == null || discoveriesOfType.size() <= 1) {
            return registryType;
        }

        // 如果同类型有多个，添加索引以区分
        int index = discoveriesOfType.indexOf(discovery);
        return String.format("%s-%d", registryType, index + 1);
    }


    /**
     * 检查实例是否包含指定元数据
     */
    private boolean hasMetadata(AirServiceInstance instance, Map<String, String> expectedMetadata) {
        Map<String, String> actualMetadata = instance.getMetadata();
        return expectedMetadata.entrySet().stream()
                .allMatch(entry -> {
                    String actualValue = actualMetadata.get(entry.getKey());
                    return entry.getValue().equals(actualValue);
                });
    }

    /**
     * 处理订阅错误
     */
    private Flux<AirServiceInstanceEvent> handleSubscriptionError(Throwable error) {
        log.warn("服务订阅失败", error);
        publishSubscriptionErrorEvent(error);
        return Flux.empty();
    }

    /**
     * 配置验证
     */
    private Mono<Void> validateConfig() {
        return Mono.fromRunnable(() -> {
            if (multiRegistryConfig == null || multiRegistryConfig.getConfigs().isEmpty()) {
                throw new IllegalStateException("未配置任何注册中心");
            }

            multiRegistryConfig.getConfigs().forEach((name, config) -> {
                if (!config.isEnabled()) return;

                if (StringUtils.isBlank(config.getType())) {
                    throw new IllegalArgumentException("注册中心类型不能为空: " + name);
                }
                // 更多验证...
            });
        });
    }


    /**
     * 构建重试策略
     */
    private Retry buildRetryStrategy(String serviceId, int maxAttempts, Duration delay) {
        return Retry.backoff(maxAttempts, delay)
                .doBeforeRetry(retrySignal ->
                        log.warn("刷新缓存重试: {}, 第 {} 次尝试",
                                serviceId, retrySignal.totalRetries() + 1))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    throw new RuntimeException(
                            "刷新服务缓存失败: " + serviceId + ", 重试次数: " + retrySignal.totalRetries(),
                            retrySignal.failure());
                });
    }

    /**
     * 刷新单个缓存（用于批量操作）
     */
    private Mono<Void> refreshSingleCache(String serviceId) {
        return refreshCache(serviceId)
                .onErrorResume(error -> {
                    log.warn("刷新服务缓存失败: {}", serviceId, error);
                    return Mono.empty(); // 继续刷新其他服务
                });
    }

    // ==================== 内部类 ====================

    /**
     * 按类型分组的注册中心健康状态
     */
    public static class RegistryHealthByType {
        private final String registryType;
        private final List<AirRegistryCenterHealth> healthList;
        private final long checkTime;

        public RegistryHealthByType(String registryType, List<AirRegistryCenterHealth> healthList) {
            this.registryType = registryType;
            this.healthList = healthList;
            this.checkTime = System.currentTimeMillis();
        }

        public String getRegistryType() {
            return registryType;
        }

        public List<AirRegistryCenterHealth> getHealthList() {
            return healthList;
        }

        public long getCheckTime() {
            return checkTime;
        }

        public boolean isOverallHealthy() {
            return healthList.stream().anyMatch(AirRegistryCenterHealth::isHealthy);
        }

        public long getHealthyCount() {
            return healthList.stream().filter(AirRegistryCenterHealth::isHealthy).count();
        }

        public int getTotalCount() {
            return healthList.size();
        }
    }

    // ==================== 事件发布方法 ====================

    private void publishManagerInitializedEvent(int registryCount) {
        // 实现管理器初始化完成事件发布
    }

    private void publishManagerInitFailedEvent(Throwable error) {
        // 实现管理器初始化失败事件发布
    }

    private void publishRegistryRegisteredEvent(String name, String type, AirReactiveServiceDiscovery discovery) {
        // 实现注册中心注册事件发布
    }

    private void publishRegistryInitFailedEvent(String name, String type, Throwable error) {
        // 实现注册中心初始化失败事件发布
    }

    private void publishDiscoveryEvent(String serviceId, List<AirServiceInstance> instances) {
        AirServiceDiscoveryEvent event = new AirServiceDiscoveryEvent(serviceId, instances);
        eventPublisher.publishEvent(event);
    }

    private void publishDiscoveryErrorEvent(AirReactiveServiceDiscovery discovery, String serviceId, Throwable error) {
        // 实现服务发现错误事件发布
    }

    private void publishCacheRefreshEvent(String serviceId, int instanceCount, boolean success, Throwable error) {
        // 实现缓存刷新事件发布
    }

    private void publishBatchCacheRefreshEvent(List<String> serviceIds, boolean success, Throwable error) {
        // 实现批量缓存刷新事件发布
    }

    private void publishSubscriptionErrorEvent(Throwable error) {
        // 实现订阅错误事件发布
    }


    // ==================== 日志方法 =======================

    private void logRefreshSuccess(String serviceId, int instanceCount) {
        log.info("刷新服务缓存完成: {}, 实例数量: {}", serviceId, instanceCount);
    }

    private void logRefreshError(String serviceId, Throwable error) {
        log.error("刷新服务缓存失败: {}", serviceId, error);
    }

    // ==================== 资源关闭 ====================
    @PreDestroy
    public Mono<Void> shutdown() {
        return Flux.fromIterable(serviceDiscoveries.values())
                .flatMap(Flux::fromIterable)
                .flatMap(discovery -> {
                    if (discovery instanceof DisposableBean) {
                        try {
                            ((DisposableBean) discovery).destroy();
                        } catch (Exception e) {
                            log.info("服务发现管理器关闭失败！！！");
                            e.printStackTrace();
                        }
                    }
                    return Mono.empty();
                })
                .then()
                .doOnSuccess(v -> {
                    //清理缓存
                    cache.clear();
                    serviceDiscoveries.clear();
                    initialized = false;
                    // 清理健康检查缓存
                    serviceInstanceHealthCheckService.clearCache();
                    registryCenterHealthService.clearCache();
                    log.info("服务发现管理器已关闭");
                    // 发布关闭事件
                    publishManagerShutdownEvent();
                });
    }

    private void publishManagerShutdownEvent() {
        // 实现管理器关闭事件发布
    }

    // ==================== 状态检查方法 ====================

    /**
     * 检查是否已初始化
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * 获取注册中心数量统计
     */
    public Map<String, Integer> getRegistryStatistics() {
        return serviceDiscoveries.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().size()
                ));
    }
}

