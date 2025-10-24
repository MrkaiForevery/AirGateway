package com.airfree.discovery;

import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public interface AirReactiveServiceDiscovery  {

    /**
     * 获取注册中心类型
     */
    String getRegistryType();

    /**
     * 初始化客户端
     */
    Mono<Void> initialize(AirRegistryCenterConfig config);

    /**
     * 获取所有服务名称
     */
    Flux<String> getServiceNames();

    /**
     * 发现服务实例
     */
    Flux<AirServiceInstance> getInstances(String serviceId);

    /**
     * 订阅服务变化
     */
    Flux<AirServiceInstanceEvent> subscribe(String serviceId);

    /**
     * 获取服务实例（带缓存）
     */
    default Flux<AirServiceInstance> getInstancesCached(String serviceId) {
        return getInstances(serviceId)
                .cache(Duration.ofSeconds(30));
    }

    /**
     * 检查健康状态
     */
    Mono<Boolean> isHealthy();
}
