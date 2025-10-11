package com.airfree.access.config;

import com.airfree.core.engine.AirGatewayEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Mono;

import java.util.Map;

@Slf4j
@Configuration
@EnableWebFlux
public class AirGatewayEndpointConfig {

    private final AirGatewayEngine airGatewayEngine;

    //通过构造器注入
    public AirGatewayEndpointConfig(AirGatewayEngine airGatewayEngine) {
        this.airGatewayEngine = airGatewayEngine;
        log.info("🚀 主程序AirGatewayEndpoint加载完成！");
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public RouterFunction<ServerResponse> airGatewayEndpoint() {
        log.info("🚀 初始化最高优先级网关路由");
        return RouterFunctions
                .route(RequestPredicates.path("/api/**"), this::handleApiRequest)
                .andRoute(RequestPredicates.path("/admin/**"), this::handleAdminRequest)
                .andRoute(RequestPredicates.path("/ai/**"), this::handleAiRequest)
                .andRoute(RequestPredicates.path("/websocket/**"), this::handleWebsocketRequest)
                .andRoute(RequestPredicates.path("/smpp/**"), this::handleSmppRequest)
                .andRoute(RequestPredicates.path("/backend/**"), this::handleBackendRequest)
                .filter((request, next) -> {
                    log.info("🟡 路由匹配 - 路径: {}, 方法: {}", request.path(), request.method());
                    return next.handle(request)
                            .doOnSuccess(response -> {
                                log.info("🟢 路由处理成功 - 路径: {}, 状态: {}",
                                        request.path(), response.statusCode());
                            })
                            .doOnError(error -> {
                                log.error("🔴 路由处理失败 - 路径: {}, 错误: {}",
                                        request.path(), error.getMessage());
                            });
                });
    }

    private Mono<ServerResponse> handleApiRequest(ServerRequest serverRequest) {
        log.info("进入该方法");
        return airGatewayEngine.routeApiRequest(serverRequest);
    }

    private Mono<ServerResponse> handleAdminRequest(ServerRequest serverRequest) {
        System.out.println("进入该方法" + serverRequest.exchange().getRequest().getId());
        return airGatewayEngine.routeAdminRequest(serverRequest);
    }

    private Mono<ServerResponse> handleAiRequest(ServerRequest serverRequest) {
        return airGatewayEngine.routeAiRequest(serverRequest);
    }

    private Mono<ServerResponse> handleWebsocketRequest(ServerRequest serverRequest) {
        return airGatewayEngine.routeWebsocketRequest(serverRequest);
    }

    private Mono<ServerResponse> handleSmppRequest(ServerRequest serverRequest) {
        return airGatewayEngine.routeSmppRequest(serverRequest);
    }

    private Mono<ServerResponse> handleBackendRequest(ServerRequest serverRequest) {
        return airGatewayEngine.forwardToBackend(serverRequest);
    }

    //这个监听器可以放到监听模块里面实现
    @EventListener
    public void printRoutes(ApplicationReadyEvent event) {
        RouterFunctions.route().toString(); // 这并不直接打印所有路由，但你可以通过调试查看

        // 或者使用以下方法获取所有路由函数
        org.springframework.web.reactive.function.server.RouterFunction<?> routerFunction =
                event.getApplicationContext().getBean(org.springframework.web.reactive.function.server.RouterFunction.class);
        // 但是注意，如果有多个 RouterFunction，getBean 可能会报错，你可以通过 getBeansOfType 获取所有
        Map<String, RouterFunction> routers = event.getApplicationContext().getBeansOfType(RouterFunction.class);
        routers.forEach((name, router) -> {
            log.info("RouterFunction bean name: {} ", name);
            log.info("RouterFunction: {} ", router);
        });
    }
}
