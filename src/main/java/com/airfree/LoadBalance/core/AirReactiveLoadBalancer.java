package com.airfree.LoadBalance.core;

import com.airfree.LoadBalance.strategy.AirLoadBalanceStrategy;
import com.airfree.LoadBalance.strategy.defaultImpl.AirRandomStrategy;
import com.airfree.LoadBalance.strategy.defaultImpl.AirRoundRobinStrategy;
import com.airfree.LoadBalance.strategy.defaultImpl.AirWeightedRoundRobinStrategy;
import com.airfree.discovery.core.AirReactiveServiceDiscoveryManager;
import com.airfree.discovery.instance.AirServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import javax.naming.ServiceUnavailableException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirReactiveLoadBalancer {

    private final AirReactiveServiceDiscoveryManager discoveryManager;
    private final Map<String, AirLoadBalanceStrategy> strategies = new ConcurrentHashMap<>();

    public AirReactiveLoadBalancer(AirReactiveServiceDiscoveryManager discoveryManager) {
        this.discoveryManager = discoveryManager;

        // 注册负载均衡策略, todo 这里看可不可以做一个factory去外部加载所有的策略，包括客户自定义的策略
        strategies.put("round-robin", new AirRoundRobinStrategy());
        strategies.put("random", new AirRandomStrategy());
        strategies.put("weighted", new AirWeightedRoundRobinStrategy());
    }

    /**
     * 选择服务实例
     */
    public Mono<AirServiceInstance> choose(String serviceId, String strategyName, ServerWebExchange exchange) {
        AirLoadBalanceStrategy strategy = strategies.getOrDefault(strategyName, strategies.get("round-robin"));

        return discoveryManager.getHealthyInstances(serviceId)
                .collectList()
                .flatMap(instances -> {
                    if (instances.isEmpty()) {
                        return Mono.error(new ServiceUnavailableException("没有可用的服务实例: " + serviceId));
                    }
                    return strategy.choose(instances, exchange);
                });
    }

}
