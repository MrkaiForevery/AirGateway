package com.airfree.LoadBalance.strategy.defaultImpl;

import com.airfree.LoadBalance.strategy.AirLoadBalanceStrategy;
import com.airfree.discovery.instance.AirServiceInstance;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 轮询策略
 */
public class AirRoundRobinStrategy implements AirLoadBalanceStrategy {

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public Mono<AirServiceInstance> choose(List<AirServiceInstance> instances, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            long index = counter.getAndIncrement() % instances.size();
            return instances.get((int) index);
        });
    }
}
