package com.airfree.discovery.core;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.cache.AirServiceDiscoveryCache;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.config.MultiAirRegistryCenterConfig;
import com.airfree.discovery.event.AirServiceDiscoveryEvent;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mongodb.core.index.MongoMappingEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

@Slf4j
@Component
public class AirReactiveServiceDiscoveryManager {

    //============= 依赖注入 =================
    private final AirReactiveRegistryCenterFactory registryFactory;
    private final MultiAirRegistryCenterConfig multiRegistryConfig;
    private final AirServiceDiscoveryCache cache;

    //============= 内部状态指标 =================
    private final Map<String, List<AirReactiveServiceDiscovery>> serviceDiscoveries = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;
    private final Mono<Void> initializationMono;

    //============= 性能指标 ====================
    private final Counter discoveryCounter;
    private final Timer discoveryTimer;

    //============= 事件发布 ====================
    //todo 这个事件发布器，统一编写实现类listener进行搞，要规整，还要在构造方法里面初始化
    private final ApplicationEventPublisher eventPublisher = null;

    //============= 构造方法 =================
    public AirReactiveServiceDiscoveryManager(AirReactiveRegistryCenterFactory registryFactory,
                                              MultiAirRegistryCenterConfig multiRegistryConfig) {
        this.registryFactory = registryFactory;
        this.multiRegistryConfig = multiRegistryConfig;
        //todo 这里先写死构造方法的两个参数值，以后再从外部读取
        this.cache = new AirServiceDiscoveryCache(30, 1000);
        initializationMono = initialize().cache(); //todo 这里异步初始化，需要有个标志句柄表示初始化是否完全完成
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
                            log.info("服务发现管理器初始化完成，注册中心数量: {}", serviceDiscoveries.values().stream().mapToInt(List::size).sum());
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
    }

    /**
     * 处理发现客户端初始化错误
     */
    private Mono<AirReactiveServiceDiscovery> handleDiscoveryInitError(String name, String type, Throwable error) {
        log.error("注册中心客户端初始化失败: {} - {}", name, type, error);
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
                        })
                ))
                .thenMany(cache.getOrLoad(serviceId, this::fetchInstancesFromAllRegistries));
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
        return Flux.empty();
    }

    // ==================== 服务实例缓存管理 ====================

    /**
     * 缓存定期刷新机制
     */
    public Mono<Void> scheduleCacheRefresh(String serviceId, Duration interval) {
        return Flux.interval(interval)
                .flatMap(tick -> refreshCache(serviceId))
                .then();
    }

    /**
     * 强制刷新缓存（立即刷新）
     */
    public Mono<Void> refreshCache(String serviceId) {
        log.info("开始刷新服务缓存: {}", serviceId);

        return ensureInitialized()
                .then(Mono.fromRunnable(() -> cache.invalidateCache(serviceId)))
                .then(discoverService(serviceId).collectList())
                .doOnSuccess(instances -> logRefreshSuccess(serviceId, instances.size()))
                .doOnError(error -> logRefreshError(serviceId, error))
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
                .doOnSuccess(instances -> logRefreshSuccess(serviceId, instances.size()))
                .doOnError(error -> logRefreshError(serviceId, error))
                .then();
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
     * 批量刷新多个服务的缓存
     */
    public Mono<Void> refreshMultipleCaches(List<String> serviceIds) {
        log.info("批量刷新服务缓存: {}", serviceIds);

        return ensureInitialized()
                .then(Flux.fromIterable(serviceIds)
                        .flatMap(this::refreshSingleCache, 3) // 并发度为3
                        .then())
                .doOnSuccess(v -> log.info("批量刷新完成，共 {} 个服务", serviceIds.size()))
                .doOnError(error -> log.error("批量刷新过程中发生错误", error));
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

    /**
     * 获取缓存统计信息
     */
    public Mono<Map<String, Object>> getCacheStats() {
        return Mono.fromCallable(cache::getStats);
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
     * 获取健康的服务实例
     */
    public Flux<AirServiceInstance> getHealthyInstances(String serviceId) {
        return discoverService(serviceId)
                .filter(AirServiceInstance::isHealthy);
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

    // ==================== 健康状态检查 ====================

    /**
     * 获取所有注册中心的健康状态
     */
    public Flux<RegistryHealth> getRegistryHealth() {
        return ensureInitialized()
                .thenMany(Flux.fromIterable(serviceDiscoveries.values())
                        .flatMap(Flux::fromIterable)
                        .flatMap(this::checkDiscoveryHealth));
    }

    /**
     * 检查单个发现客户端的健康状态
     */
    private Mono<RegistryHealth> checkDiscoveryHealth(AirReactiveServiceDiscovery discovery) {
        return discovery.isHealthy()
                .map(healthy -> new RegistryHealth(discovery.getRegistryType(), healthy))
                .onErrorReturn(new RegistryHealth(discovery.getRegistryType(), false));
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 确保服务已初始化
     */
    private Mono<Void> ensureInitialized() {
        return initializationMono;
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
        return Flux.empty();
    }

    /**
     * 外部输入的配置校验
     */
// 添加配置验证
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
                // todo 更多验证...
            });
        });
    }


    // ==================== 日志方法 ====================

    private void logDiscoverySuccess(String serviceId, AirServiceInstance instance) {
        log.debug("发现服务实例: {} -> {}:{}", serviceId, instance.getHost(), instance.getPort());
    }

    private void logDiscoveryComplete(String serviceId) {
        log.info("服务发现完成: {}, 总计注册中心: {}", serviceId, serviceDiscoveries.values().stream().mapToInt(List::size).sum());
    }

    private void logRefreshSuccess(String serviceId, int instanceCount) {
        log.info("刷新服务缓存完成: {}, 实例数量: {}", serviceId, instanceCount);
    }

    private void logRefreshError(String serviceId, Throwable error) {
        log.error("刷新服务缓存失败: {}", serviceId, error);
    }

    // ==================== 内部类 ====================

    /**
     * 注册中心健康状态
     */
    public static class RegistryHealth {
        private final String registryType;
        private final boolean healthy;

        public RegistryHealth(String registryType, boolean healthy) {
            this.registryType = registryType;
            this.healthy = healthy;
        }

        public String getRegistryType() {
            return registryType;
        }

        public boolean isHealthy() {
            return healthy;
        }
    }

    // ==================== 事件发布 ====================

    /**
     * * 服务实例更新批量事件发布
     */
    private void publishDiscoveryEvent(String serviceId, List<AirServiceInstance> instances) {
        AirServiceDiscoveryEvent event = new AirServiceDiscoveryEvent(serviceId, instances);
        eventPublisher.publishEvent(event);
    }

    // ==================== 健康检查 ====================



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
                    cache.clear();
                    serviceDiscoveries.clear();
                    initialized = false;
                    log.info("服务发现管理器已关闭");
                });
    }
}

