package com.airfree.discovery.core;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.enums.AirServiceDiscoveryTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.lang.reflect.Constructor;
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
                // 通过枚举查找对应的类
                AirServiceDiscoveryTypeEnum typeEnum = AirServiceDiscoveryTypeEnum.findDiscoveryTypeEnum(type);
                if (typeEnum == null) {
                    throw new IllegalArgumentException("不支持的注册中心类型: " + type);
                }
                // 使用反射创建实例
                Class<? extends AirReactiveServiceDiscovery> clazz = typeEnum.getReflectClass();
                try {
                    Constructor<? extends AirReactiveServiceDiscovery> constructor = clazz.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    AirReactiveServiceDiscovery discovery = constructor.newInstance();
                    log.debug("通过反射创建注册中心实例成功: {} -> {}", type, clazz.getSimpleName());
                    return discovery;
                } catch (Exception e) {
                    log.error("创建注册中心实例失败: {}", type, e);
                    throw new RuntimeException("创建注册中心实例失败: " + type, e);
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
     * 获取活跃的发现客户端
     */
    public Map<String, AirReactiveServiceDiscovery> getActiveDiscoveries() {
        return new ConcurrentHashMap<>(activeDiscoveries);
    }

    /**
     * 清理缓存
     */
    @PreDestroy
    public void clearCache() {
        discoveryMap.clear();
        activeDiscoveries.clear();
        log.debug("注册中心工厂缓存已清理");
    }

}
