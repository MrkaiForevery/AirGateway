package com.airfree.router.core;

import com.airfree.discovery.core.AirReactiveServiceDiscoveryManager;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 多注册中心路由定位器,这里使用spring-cloud-gateway的locator来管理
 */
@Slf4j
@Component
public class AirMultiRegistryRouteDefinitionLocator implements RouteDefinitionLocator {

    private final AirReactiveServiceDiscoveryManager discoveryManager;
    private final Flux<RouteDefinition> routeDefinitionFlux;
    private final Map<String, List<RouteDefinition>> serviceRoutes = new ConcurrentHashMap<>();


    public AirMultiRegistryRouteDefinitionLocator(AirReactiveServiceDiscoveryManager discoveryManager) {
        this.discoveryManager = discoveryManager;
        // 创建反应式路由定义流
        this.routeDefinitionFlux = createRouteDefinitionFlux();
    }

    private Flux<RouteDefinition> createRouteDefinitionFlux() {
        // 初始加载所有路由
        Flux<RouteDefinition> initialRoutes = discoveryManager.getAllServiceNames()
                .flatMap(this::createRouteDefinitionsForService)
                .doOnNext(this::enhanceRouteWithRPCInfo) // 增强路由信息给rpcManager提取协议使用
                .doOnNext(route -> log.debug("创建路由: {}", route.getId()))
                .cache();

        // 监听服务变化，动态更新路由
        Flux<RouteDefinition> dynamicRoutes = discoveryManager.getAllServiceNames()
                .flatMap(serviceId -> discoveryManager.subscribeService(serviceId)
                        .flatMap(event -> handleServiceInstanceEvent(event, serviceId)))
                .filter(Objects::nonNull);

        // 合并静态路由和动态路由
        return Flux.merge(initialRoutes, dynamicRoutes)
                .distinct(RouteDefinition::getId)
                .cache(); // 缓存路由定义
    }


    /**
     * 为服务创建路由定义
     */
    private Flux<RouteDefinition> createRouteDefinitionsForService(String serviceId) {
        return discoveryManager.discoverService(serviceId)
                .take(1)
                .next() // 取第一个实例转换为 Mono
                .flatMapMany(instance ->
                        Mono.fromCallable(() -> buildRouteDefinitions(serviceId, instance))
                                .flatMapIterable(Function.identity())
                                .doOnNext(route -> cacheServiceRoute(serviceId, route))
                                .onErrorResume(e -> {
                                    log.warn("为服务 {} 创建路由失败", serviceId, e);
                                    return Flux.empty();
                                })
                )
                .switchIfEmpty(Mono.defer(() -> {
                    log.debug("服务 {} 没有可用实例，跳过路由创建", serviceId);
                    return Mono.empty();
                }));
    }


    /**
     * 构建路由定义列表
     */
    private List<RouteDefinition> buildRouteDefinitions(String serviceId, AirServiceInstance instance) {
        List<RouteDefinition> routes = new ArrayList<>();

        // 默认路由
        routes.add(createDefaultRoute(serviceId, instance));

        // 基于元数据的自定义路由
        Map<String, String> metadata = instance.getMetadata();
        if (metadata != null) {
            // 自定义路径路由
            String customPath = metadata.get("gateway.path");
            if (isValidPath(customPath)) {
                routes.add(createCustomRoute(serviceId, customPath, instance));
            }

            // 版本路由
            String version = metadata.get("version");
            if (isValidVersion(version)) {
                routes.add(createVersionRoute(serviceId, version, instance));
            }

            // 权重路由
            String weight = metadata.get("weight");
            if (isValidWeight(weight)) {
                routes.add(createWeightedRoute(serviceId, weight, instance));
            }
        }

        log.debug("为服务 {} 创建了 {} 个路由定义", serviceId, routes.size());
        return routes;
    }


    /**
     * 处理服务实例事件
     */
    private Mono<RouteDefinition> handleServiceInstanceEvent(AirServiceInstanceEvent event, String serviceId) {
        log.info("处理服务实例事件: {} -> {}", serviceId, event.getEventType());

        switch (event.getEventType()) {
            case ADDED:
            case MODIFIED:
                // 重新创建该服务的路由
                return createRouteDefinitionsForService(serviceId)
                        .next() // 取第一个路由作为事件响应
                        .doOnNext(route -> log.info("更新路由: {}", route.getId()));

            case REMOVED:
                // 移除该服务的路由
                return removeServiceRoutes(serviceId);

            default:
                return Mono.empty();
        }
    }

    /**
     * 移除服务的所有路由
     */
    private Mono<RouteDefinition> removeServiceRoutes(String serviceId) {
        return Mono.fromRunnable(() -> {
            List<RouteDefinition> removedRoutes = serviceRoutes.remove(serviceId);
            if (removedRoutes != null) {
                log.info("移除服务 {} 的 {} 个路由", serviceId, removedRoutes.size());
                // 在实际网关实现中，这里应该触发路由删除逻辑
                // 例如通过 ApplicationEventPublisher 发布路由删除事件
            }
        }).then(Mono.empty());
    }


    /**
     * 缓存服务路由
     */
    private void cacheServiceRoute(String serviceId, RouteDefinition route) {
        serviceRoutes.computeIfAbsent(serviceId, k -> new ArrayList<>())
                .add(route);
    }


    // ==================== 路由创建方法 ====================
    private RouteDefinition createDefaultRoute(String serviceId, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-default");
        route.setUri(createServiceUri(serviceId));

        // 路径断言：/serviceId/**
        PredicateDefinition pathPredicate = new PredicateDefinition();
        pathPredicate.setName("Path");
        pathPredicate.addArg("pattern", "/" + serviceId + "/**");
        route.setPredicates(Collections.singletonList(pathPredicate));

        // 重写路径过滤器：移除服务名前缀
        FilterDefinition rewriteFilter = new FilterDefinition();
        rewriteFilter.setName("RewritePath");
        rewriteFilter.addArg("regexp", "/" + serviceId + "/(?<segment>.*)");
        rewriteFilter.addArg("replacement", "/${segment}");
        route.setFilters(Collections.singletonList(rewriteFilter));

        // 添加元数据
        route.setMetadata(createRouteMetadata(instance, "default"));

        return route;
    }

    private RouteDefinition createCustomRoute(String serviceId, String customPath, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-custom-" + System.currentTimeMillis());
        route.setUri(createServiceUri(serviceId));

        PredicateDefinition predicate = new PredicateDefinition();
        predicate.setName("Path");

        // 确保路径以 / 开头
        String normalizedPath = customPath.startsWith("/") ? customPath : "/" + customPath;
        predicate.addArg("pattern", normalizedPath + "/**");
        route.setPredicates(Collections.singletonList(predicate));

        route.setMetadata(createRouteMetadata(instance, "custom"));
        return route;
    }

    private RouteDefinition createVersionRoute(String serviceId, String version, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-v" + version);
        route.setUri(createServiceUri(serviceId));

        List<PredicateDefinition> predicates = new ArrayList<>();

        // 路径断言
        PredicateDefinition pathPredicate = new PredicateDefinition();
        pathPredicate.setName("Path");
        pathPredicate.addArg("pattern", "/v" + version + "/" + serviceId + "/**");
        predicates.add(pathPredicate);

        // 版本头断言
        PredicateDefinition headerPredicate = new PredicateDefinition();
        headerPredicate.setName("Header");
        headerPredicate.addArg("header", "X-API-Version");
        headerPredicate.addArg("regexp", version);
        predicates.add(headerPredicate);

        route.setPredicates(predicates);

        // 重写路径过滤器
        FilterDefinition filter = new FilterDefinition();
        filter.setName("RewritePath");
        filter.addArg("regexp", "/v" + version + "/" + serviceId + "/(?<segment>.*)");
        filter.addArg("replacement", "/${segment}");
        route.setFilters(Collections.singletonList(filter));

        route.setMetadata(createRouteMetadata(instance, "versioned"));
        return route;
    }

    private RouteDefinition createWeightedRoute(String serviceId, String weight, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-weight-" + weight);
        route.setUri(createServiceUri(serviceId));
        route.setOrder(Integer.parseInt(weight)); // 使用权重作为路由顺序

        PredicateDefinition predicate = new PredicateDefinition();
        predicate.setName("Path");
        predicate.addArg("pattern", "/weight/" + weight + "/" + serviceId + "/**");
        route.setPredicates(Collections.singletonList(predicate));

        route.setMetadata(createRouteMetadata(instance, "weighted"));
        return route;
    }

    // ==================== 工具方法 ====================
    private URI createServiceUri(String serviceId) {
        return URI.create("lb://" + serviceId);
    }

    private Map<String, Object> createRouteMetadata(AirServiceInstance instance, String routeType) {
        Map<String, Object> metadata = new HashMap<>();
        if (instance.getMetadata() != null) {
            metadata.putAll(instance.getMetadata());
        }
        metadata.put("routeType", routeType);
        metadata.put("createdTime", System.currentTimeMillis());
        return metadata;
    }

    private boolean isValidPath(String path) {
        return path != null && !path.trim().isEmpty();
    }

    private boolean isValidVersion(String version) {
        return version != null && !version.trim().isEmpty();
    }

    private boolean isValidWeight(String weight) {
        if (weight == null || weight.trim().isEmpty()) {
            return false;
        }
        try {
            int w = Integer.parseInt(weight);
            return w > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void logRouteDefinition(RouteDefinition route) {
        if (log.isDebugEnabled()) {
            log.debug("路由定义: id={}, uri={}, predicates={}, filters={}",
                    route.getId(), route.getUri(), route.getPredicates(), route.getFilters());
        }
    }

    // ==================== 公共方法 ====================
    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        return routeDefinitionFlux;
    }

    /**
     * 获取服务路由统计
     */
    public Map<String, Integer> getRouteStatistics() {
        Map<String, Integer> stats = new HashMap<>();
        serviceRoutes.forEach((serviceId, routes) -> {
            stats.put(serviceId, routes.size());
        });
        return stats;
    }

    /**
     * 手动刷新路由
     */
    public Mono<Void> refreshRoutes() {
        return discoveryManager.getAllServiceNames()
                .flatMap(this::createRouteDefinitionsForService)
                .then()
                .doOnSuccess(v -> log.info("路由刷新完成"));
    }


    // ==================== 增强路由定义方法 ====================
    /**
     * 增强路由定义，添加RPC相关信息
     */
    private void enhanceRouteWithRPCInfo(RouteDefinition route) {
        Map<String, Object> metadata = route.getMetadata();
        if (metadata == null) {
            metadata = new HashMap<String,Object>();
            route.setMetadata(metadata);
        }

        // 从服务实例元数据提取RPC协议
        String serviceId = extractServiceId(route);
        if (serviceId != null) {
            Map<String, Object> finalMetadata = metadata;
            discoveryManager.discoverService(serviceId)
                    .take(1)
                    .next()
                    .subscribe(instance -> {
                        Map<String, String> instanceMetadata = instance.getMetadata();
                        if (instanceMetadata != null) {
                            // 设置RPC协议
                            String rpcProtocol = instanceMetadata.get("rpc.protocol");
                            if (rpcProtocol != null) {
                                finalMetadata.put("rpc.protocol", rpcProtocol);
                            }

                            // 设置序列化方式
                            String serialization = instanceMetadata.get("rpc.serialization");
                            if (serialization != null) {
                                finalMetadata.put("rpc.serialization", serialization);
                            }

                            // 设置超时配置
                            String timeout = instanceMetadata.get("rpc.timeout");
                            if (timeout != null) {
                                finalMetadata.put("rpc.timeout", timeout);
                            }
                        }
                    });
        }
    }

    private String extractServiceId(RouteDefinition route) {
        URI uri = route.getUri();
        if ("lb".equals(uri.getScheme())) {
            return uri.getHost();
        }
        return null;
    }
}
