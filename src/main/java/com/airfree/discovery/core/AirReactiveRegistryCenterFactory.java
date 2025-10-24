package com.airfree.discovery.core;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.discoveryService.AirConsulReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirEurekaReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirNacosReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirZookeeperReactiveDiscovery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirReactiveRegistryCenterFactory {

    private final Map<String, AirReactiveServiceDiscovery> discoveryMap = new ConcurrentHashMap<>();
    private final Map<String, AirReactiveServiceDiscovery> activeDiscoveries = new ConcurrentHashMap<>();

    /**
     * 创建注册中心客户端
     */
    public Mono<AirReactiveServiceDiscovery> createDiscovery(AirRegistryCenterConfig config) {
        return Mono.fromCallable(() -> {
            String type = config.getType().toLowerCase();

            return discoveryMap.computeIfAbsent(type, k -> {
                switch (type) {
                    case "nacos":
                        return new AirNacosReactiveDiscovery();
                    case "zookeeper":
                        return new AirZookeeperReactiveDiscovery();
                    case "consul":
                        return new AirConsulReactiveDiscovery();
                    case "eureka":
                        return new AirEurekaReactiveDiscovery();
                    default:
                        throw new IllegalArgumentException("不支持的注册中心类型: " + type);
                }
            });
        }).flatMap(discovery -> discovery.initialize(config)
                .doOnSuccess(v -> {
                    activeDiscoveries.put(config.getType(), discovery);
                    log.info("注册中心初始化成功: {}", config.getType());
                })
                .thenReturn(discovery));
    }

    /**
     * 获取所有活跃的发现客户端
     */
    public Flux<AirReactiveServiceDiscovery> getActiveDiscoveries() {
        return Flux.fromIterable(activeDiscoveries.values());
    }

    /**
     * 根据类型获取发现客户端
     */
    public Mono<AirReactiveServiceDiscovery> getDiscovery(String type) {
        return Mono.justOrEmpty(activeDiscoveries.get(type));
    }

}
