package com.airfree.filter;

import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Map;

public interface AirGatewayFilterManager {

      void  buildAllFilterChainByStrategy(Map<String, AirGatewayStrategy> customerFilterStrategyMap);

      Mono<Void>  executeFilterChain(AirGatewayStrategy strategy, ServerWebExchange exchange, WebFilterChain chain);

      public AirGatewayStrategy findAirGatewayStrategyName(String strategyName);
}
