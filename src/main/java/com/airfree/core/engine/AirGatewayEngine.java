package com.airfree.core.engine;

import com.airfree.core.AirGateway;
import com.airfree.filter.AirGatewayFilterManager;
import com.airfree.filter.AirGatewayStrategy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class AirGatewayEngine implements AirGateway {

    //这里可以用构造器的方式注入所需要的流控，性能，监控等动态配置缓存
    @Resource
    @Qualifier("customerAirGatewayFilterManager")
    private AirGatewayFilterManager customerAirGatewayFilterManager;


    @Override
    public Mono<ServerResponse> routeApiRequest(ServerRequest serverRequest) {
        //自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");

        return customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null)
                .then(ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp")))
                .onErrorResume(throwable -> handleError(throwable));
    }

    //这里先用mock的方式替代
    private Map<String, Object> mockCreateSuccessResponse(int status, String message, String data) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", 200);
        response.put("message", "api request processed successfully");
        response.put("data", "api");
        return response;
    }

    // 错误处理方法
    private Mono<ServerResponse> handleError(Throwable throwable) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", 500);
        errorResponse.put("message", "Error processing request: " + throwable.getMessage());

        return ServerResponse.status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(errorResponse);
    }

    @Override
    public Mono<ServerResponse> routeAdminRequest(ServerRequest serverRequest) {
        //自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        return Mono.fromCallable(() -> {
                    AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
                    customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);
                    return exchange;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(processedExchange -> {
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp"));
                })
                .onErrorResume(throwable -> handleError(throwable));
    }

    @Override
    public Mono<ServerResponse> routeAiRequest(ServerRequest serverRequest) {
        //自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        return Mono.fromCallable(() -> {
                    AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
                    customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);
                    return exchange;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(processedExchange -> {
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp"));
                })
                .onErrorResume(throwable -> handleError(throwable));
    }

    @Override
    public Mono<ServerResponse> routeWebsocketRequest(ServerRequest serverRequest) {
        //自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        return Mono.fromCallable(() -> {
                    AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
                    customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);
                    return exchange;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(processedExchange -> {
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp"));
                })
                .onErrorResume(throwable -> handleError(throwable));
    }

    @Override
    public Mono<ServerResponse> routeSmppRequest(ServerRequest serverRequest) {
        //自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        return Mono.fromCallable(() -> {
                    AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
                    customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);
                    return exchange;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(processedExchange -> {
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp"));
                })
                .onErrorResume(throwable -> handleError(throwable));
    }

    @Override
    public Mono<ServerResponse> forwardToBackend(ServerRequest serverRequest) {
        ///自定义customerFilter执行逻辑
        ServerWebExchange exchange = serverRequest.exchange();
        return Mono.fromCallable(() -> {
                    AirGatewayStrategy apiFilterStrategy = customerAirGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
                    customerAirGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);
                    return exchange;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(processedExchange -> {
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(mockCreateSuccessResponse(200, "api request processed successfully", "apiResp"));
                })
                .onErrorResume(throwable -> handleError(throwable));
    }


}
