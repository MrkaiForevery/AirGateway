package com.airfree.LoadBalance.strategy.defaultImpl;

import com.airfree.LoadBalance.strategy.AirLoadBalanceStrategy;
import com.airfree.discovery.instance.AirServiceInstance;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机策略
 */
public  class AirRandomStrategy  implements AirLoadBalanceStrategy {

    @Override
    public Mono<AirServiceInstance> choose(List<AirServiceInstance> instances, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            int index = ThreadLocalRandom.current().nextInt(instances.size());
            return instances.get(index);
        });
    }
}
