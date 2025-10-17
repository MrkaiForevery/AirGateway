package com.airfree.rpc.client;

import com.airfree.rpc.AirGatewayRPCServiceClient;
import com.airfree.rpc.entity.AirRPCResponse;
import com.airfree.rpc.entity.AirRpcRequestContext;
import com.airfree.rpc.entity.AirServiceEndpoint;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Component
public class HttpServiceClient implements AirGatewayRPCServiceClient {

    private final WebClient.Builder webClientBuilder;

    public HttpServiceClient(WebClient.Builder webClientBuilder) {
        this.webClientBuilder = webClientBuilder;
    }

    @Override
    public String getProtocol() {
        return AirRPCTypeEnum.HTTP.getRpcName();
    }

    @Override
    public Mono<AirRPCResponse<String>> execute(AirRpcRequestContext context) {
        return null;
    }

    @Override
    public Flux<AirRPCResponse<String>> executeStream(AirRpcRequestContext context) {
        return null;
    }

    @Override
    public boolean support(AirServiceEndpoint endpoint) {
        return  "HTTP".equals(endpoint.getProtocol());
    }
}
