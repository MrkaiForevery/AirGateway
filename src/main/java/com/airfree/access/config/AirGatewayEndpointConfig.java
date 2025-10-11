package com.airfree.access.config;

import com.airfree.core.engine.AirGatewayEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Mono;

@Slf4j
@Configuration
@EnableWebFlux
public class AirGatewayEndpointConfig {

    private final AirGatewayEngine airGatewayEngine;

    // 通过构造函数注入依赖
    public AirGatewayEndpointConfig(AirGatewayEngine airGatewayEngine) {
        this.airGatewayEngine = airGatewayEngine;
    }

    @Bean
    @Order(-1)
    public RouterFunction<ServerResponse> gatewayEndpoint() {
        return RouterFunctions
                .route(RequestPredicates.path("/api/**"), this::handleApiRequest)
                .andRoute(RequestPredicates.path("/admin/**"), this::handleAdminRequest)
                .andRoute(RequestPredicates.path("/ai/**"), this::handleAiRequest)
                .andRoute(RequestPredicates.path("/websocket/**"), this::handleWebsocketRequest)
                .andRoute(RequestPredicates.path("/smpp/**"), this::handleSmppRequest)
                .andRoute(RequestPredicates.path("/backend/**"), this::handleBackendRequest);
    }

    private Mono<ServerResponse> handleApiRequest(ServerRequest serverRequest) {
        log.info("处理api请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        Mono<ServerResponse> responseMono = airGatewayEngine.routeApiRequest(serverRequest);
        return responseMono;
//        return  airGatewayEngine.routeApiRequest(serverRequest);
    }

    private Mono<ServerResponse> handleAdminRequest(ServerRequest serverRequest) {
        log.info("处理Admin请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        return airGatewayEngine.routeAdminRequest(serverRequest);
    }

    private Mono<ServerResponse> handleAiRequest(ServerRequest serverRequest) {
        log.info("处理Ai请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        return  airGatewayEngine.routeAiRequest(serverRequest);
    }

    private Mono<ServerResponse> handleWebsocketRequest(ServerRequest serverRequest) {
        log.info("处理Websocket请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        return  airGatewayEngine.routeWebsocketRequest(serverRequest);
    }

    private Mono<ServerResponse> handleSmppRequest(ServerRequest serverRequest) {
        log.info("处理Smpp请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        return  airGatewayEngine.routeSmppRequest(serverRequest);
    }

    private Mono<ServerResponse> handleBackendRequest(ServerRequest serverRequest) {
        log.info("处理Backend请求开始，requestId {}",serverRequest.exchange().getRequest().getId());
        return  airGatewayEngine.forwardToBackend(serverRequest);
    }
}
