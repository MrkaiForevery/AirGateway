package com.airfree.LoadBalance.strategy;

import com.airfree.discovery.instance.AirServiceInstance;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AirLoadBalanceStrategy {
    Mono<AirServiceInstance> choose(List<AirServiceInstance> instances, ServerWebExchange exchange);
}
