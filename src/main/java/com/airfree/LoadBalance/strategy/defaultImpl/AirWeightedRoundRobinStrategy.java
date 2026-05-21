package com.airfree.LoadBalance.strategy.defaultImpl;

import com.airfree.LoadBalance.strategy.AirLoadBalanceStrategy;
import com.airfree.discovery.instance.AirServiceInstance;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 权重策略
 */
public class AirWeightedRoundRobinStrategy implements AirLoadBalanceStrategy {

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public Mono<AirServiceInstance> choose(List<AirServiceInstance> instances, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            int totalWeight = instances.stream()
                    .mapToInt(instance ->
                            Integer.parseInt((String) instance.getMetadata().getOrDefault("weight", "1")))
                    .sum();

            if (totalWeight == 0) {
                int index = (int) (counter.getAndIncrement() % instances.size());
                return instances.get(index);
            }

            long current = counter.getAndIncrement();
            int mod = (int) (current % totalWeight);

            for (AirServiceInstance instance : instances) {
                int weight = Integer.parseInt((String) instance.getMetadata().getOrDefault("weight", "1"));
                if (mod < weight) {
                    return instance;
                }
                mod -= weight;
            }

            return instances.get(0);
        });
    }

}
