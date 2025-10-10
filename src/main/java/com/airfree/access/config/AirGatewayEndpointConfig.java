package com.airfree.access.config;

import com.airfree.core.engine.AirGatewayEngine;
import com.airfree.entity.converter.RequestConverter;
import com.airfree.entity.converter.ResponseConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;


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
        //执行过滤链条，不同的请求协议提供不同的过滤策略链

        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.routeApiRequest(serverRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }

    private Mono<ServerResponse> handleAdminRequest(ServerRequest serverRequest) {
        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.routeAdminRequest(airGatewayInternalRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }

    private Mono<ServerResponse> handleAiRequest(ServerRequest serverRequest) {
        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.routeAiRequest(airGatewayInternalRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }

    private Mono<ServerResponse> handleWebsocketRequest(ServerRequest serverRequest) {
        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.routeWebsocketRequest(airGatewayInternalRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }

    private Mono<ServerResponse> handleSmppRequest(ServerRequest serverRequest) {
        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.routeSmppRequest(airGatewayInternalRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }

    private Mono<ServerResponse> handleBackendRequest(ServerRequest serverRequest) {
        //api接口request类型对象转换
        return Mono.fromCallable(() -> RequestConverter.converterApiInternalRequest(serverRequest))
                .subscribeOn(Schedulers.boundedElastic()) // 在弹性线程池中执行阻塞操作
                .flatMap(airGatewayInternalRequest -> airGatewayEngine.forwardToBackend(airGatewayInternalRequest))
                .transform(ResponseConverter::converterApiInternalResponse);
    }
}
