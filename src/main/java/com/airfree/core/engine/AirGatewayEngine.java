package com.airfree.core.engine;

import com.airfree.core.AirGateway;
import com.airfree.entity.request.AirGatewayInternalRequest;
import com.airfree.entity.response.AirGatewayInternalResponse;
import com.airfree.filter.AirGatewayFilterManager;
import com.airfree.filter.AirGatewayStrategy;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class AirGatewayEngine implements AirGateway {

    //这里可以用构造器的方式注入所需要的流控，性能，监控等动态配置缓存
    @Resource
    private AirGatewayFilterManager airGatewayFilterManager;


    @Override
    public Mono<AirGatewayInternalResponse> routeApiRequest(ServerRequest serverRequest) {
        //filter执行逻辑
        AirGatewayStrategy apiFilterStrategy = airGatewayFilterManager.findAirGatewayStrategyName("ApiFilterStrategy");
        ServerWebExchange exchange = serverRequest.exchange();
        airGatewayFilterManager.executeFilterChain(apiFilterStrategy, exchange, null);

        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'a', 'p', 'i'};
            response.setBody(body);
            return Mono.just(response);
        });
    }

    @Override
    public Mono<AirGatewayInternalResponse> routeAdminRequest(AirGatewayInternalRequest request) {
        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'a', 'd', 'm', 'i', 'n'};
            response.setBody(body);
            return Mono.just(response);
        });
    }

    @Override
    public Mono<AirGatewayInternalResponse> routeAiRequest(AirGatewayInternalRequest request) {
        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'a', 'i'};
            response.setBody(body);
            return Mono.just(response);
        });
    }

    @Override
    public Mono<AirGatewayInternalResponse> routeWebsocketRequest(AirGatewayInternalRequest request) {
        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'w', 'e', 'b', 's', 'o', 'c', 'k', 'e', 't'};
            response.setBody(body);
            return Mono.just(response);
        });
    }

    @Override
    public Mono<AirGatewayInternalResponse> routeSmppRequest(AirGatewayInternalRequest request) {
        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'s', 'm', 'p', 'p'};
            response.setBody(body);
            return Mono.just(response);
        });
    }

    @Override
    public Mono<AirGatewayInternalResponse> forwardToBackend(AirGatewayInternalRequest request) {
        //todo 流式编程不能返回null，这里先让它返回一个默认的成功
        return Mono.defer(() -> {
            // 创建默认的成功响应
            AirGatewayInternalResponse response = new AirGatewayInternalResponse();
            response.setStatus(200);
            byte[] body = {'b', 'a', 'c', 'k', 'e', 'n', 'd'};
            response.setBody(body);
            return Mono.just(response);
        });
    }


}
