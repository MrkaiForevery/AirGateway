package com.airfree.rpc.client;

import com.airfree.rpc.AirGatewayRPCServiceClient;
import com.airfree.rpc.entity.AirRPCResponse;
import com.airfree.rpc.entity.AirRpcRequestContext;
import com.airfree.rpc.entity.AirServiceEndpoint;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class RSocketServiceClient implements AirGatewayRPCServiceClient {

    private final RSocketRequester.Builder requesterBuilder;
    private final Map<String, RSocketRequester> requesterPool = new ConcurrentHashMap<>();

    public RSocketServiceClient(RSocketRequester.Builder requesterBuilder) {
        this.requesterBuilder = requesterBuilder;
    }

    @Override
    public String getProtocol() {
        return AirRPCTypeEnum.RSOCKET.getRpcName();
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
        return "RSocket".equals(endpoint.getProtocol());
    }
}
