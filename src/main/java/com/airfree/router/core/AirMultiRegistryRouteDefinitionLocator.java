package com.airfree.router.core;

import com.airfree.discovery.core.AirReactiveServiceDiscoveryManager;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.config.GatewayProperties;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

@Slf4j
@Component
public class AirMultiRegistryRouteDefinitionLocator implements RouteDefinitionLocator {

    private final AirReactiveServiceDiscoveryManager discoveryManager;
    private final GatewayProperties gatewayProperties;
    private final Flux<RouteDefinition> routeDefinitionFlux;

    public AirMultiRegistryRouteDefinitionLocator(AirReactiveServiceDiscoveryManager discoveryManager,
                                               GatewayProperties gatewayProperties) {
        this.discoveryManager = discoveryManager;
        this.gatewayProperties = gatewayProperties;
        // 创建反应式路由定义流
        this.routeDefinitionFlux = createRouteDefinitionFlux();
    }

    private Flux<RouteDefinition> createRouteDefinitionFlux() {
        // 初始加载所有路由
        Flux<RouteDefinition> initialRoutes = discoveryManager.getAllServiceNames()
                .flatMap(this::createRouteDefinitionsForService)
                .doOnNext(route -> log.debug("创建路由: {}", route.getId()));

        // 监听服务变化，动态更新路由
        Flux<RouteDefinition> dynamicRoutes = discoveryManager.getAllServiceNames()
                .flatMap(serviceId -> discoveryManager.subscribeService(serviceId)
                        .flatMap(event -> handleServiceEvent(event, serviceId)))
                .filter(Objects::nonNull);

        // 合并静态路由和动态路由
        return Flux.merge(initialRoutes, dynamicRoutes)
                .distinct(RouteDefinition::getId)
                .cache(); // 缓存路由定义
    }


    private Flux<RouteDefinition> createRouteDefinitionsForService(String serviceId) {
        return discoveryManager.discoverService(serviceId)
                .take(1) // 只需要一个实例来获取元数据
                .flatMapMany(instance -> {
                    List<RouteDefinition> routes = new ArrayList<>();

                    // 默认路由：/serviceId/**
                    RouteDefinition defaultRoute = createDefaultRoute(serviceId, instance);
                    routes.add(defaultRoute);

                    // 基于元数据的自定义路由
                    Map<String, String> metadata = instance.getMetadata();
                    String customPath = metadata.get("gateway.path");
                    if (customPath != null) {
                        RouteDefinition customRoute = createCustomRoute(serviceId, customPath, instance);
                        routes.add(customRoute);
                    }

                    String version = metadata.get("version");
                    if (version != null) {
                        RouteDefinition versionRoute = createVersionRoute(serviceId, version, instance);
                        routes.add(versionRoute);
                    }

                    return Flux.fromIterable(routes);
                })
                .onErrorResume(e -> {
                    log.warn("为服务 {} 创建路由失败:{}", serviceId);
                    return Flux.empty();
                });
    }

    private Mono<RouteDefinition> handleServiceEvent(AirServiceInstanceEvent event, String serviceId) {
        return switch (event.getEventType()) {
            case ADDED -> createRouteDefinitionsForService(serviceId).next();
            case REMOVED -> {
                RouteDefinition route = new RouteDefinition();
                route.setId(serviceId + "-removed");
                // 在实际实现中，这里应该触发路由删除
                yield Mono.empty();
            }
            default -> Mono.empty();
        };
    }

    private RouteDefinition createDefaultRoute(String serviceId, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-default");
        route.setUri(URI.create("lb://" + serviceId));

        // 路径断言：/serviceId/**
        PredicateDefinition predicate = new PredicateDefinition();
        predicate.setName("Path");
        predicate.addArg("pattern", "/" + serviceId + "/**");
        route.setPredicates(Collections.singletonList(predicate));

        // 重写路径过滤器
        FilterDefinition filter = new FilterDefinition();
        filter.setName("RewritePath");
        filter.addArg("regexp", "/" + serviceId + "/(?<segment>.*)");
        filter.addArg("replacement", "/${segment}");
        route.setFilters(Collections.singletonList(filter));

        // 添加元数据
        route.setMetadata(instance.getMetadata());

        return route;
    }

    private RouteDefinition createCustomRoute(String serviceId, String customPath, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-custom");
        route.setUri(URI.create("lb://" + serviceId));

        PredicateDefinition predicate = new PredicateDefinition();
        predicate.setName("Path");
        predicate.addArg("pattern", customPath + "/**");
        route.setPredicates(Collections.singletonList(predicate));

        route.setMetadata(instance.getMetadata());
        return route;
    }

    private RouteDefinition createVersionRoute(String serviceId, String version, AirServiceInstance instance) {
        RouteDefinition route = new RouteDefinition();
        route.setId(serviceId + "-v" + version);
        route.setUri(URI.create("lb://" + serviceId));

        // 路径和版本头断言
        List<PredicateDefinition> predicates = new ArrayList<>();

        PredicateDefinition pathPredicate = new PredicateDefinition();
        pathPredicate.setName("Path");
        pathPredicate.addArg("pattern", "/v" + version + "/" + serviceId + "/**");
        predicates.add(pathPredicate);

        PredicateDefinition headerPredicate = new PredicateDefinition();
        headerPredicate.setName("Header");
        headerPredicate.addArg("header", "X-API-Version");
        headerPredicate.addArg("regexp", version);
        predicates.add(headerPredicate);

        route.setPredicates(predicates);

        // 重写路径
        FilterDefinition filter = new FilterDefinition();
        filter.setName("RewritePath");
        filter.addArg("regexp", "/v" + version + "/" + serviceId + "/(?<segment>.*)");
        filter.addArg("replacement", "/${segment}");
        route.setFilters(Collections.singletonList(filter));

        return route;
    }

    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        return routeDefinitionFlux;
    }
}
