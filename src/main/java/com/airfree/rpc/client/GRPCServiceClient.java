package com.airfree.rpc.client;

import com.airfree.rpc.AirGatewayRPCServiceClient;
import com.airfree.rpc.entity.AirRPCResponse;
import com.airfree.rpc.entity.AirRpcRequestContext;
import com.airfree.rpc.entity.AirServiceEndpoint;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class GRPCServiceClient implements AirGatewayRPCServiceClient {

    //TODO 这个ManagedChannel的导入的包是否是grpc的包
    private final Map<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    @Override
    public String getProtocol() {
        return AirRPCTypeEnum.GRPC.getRpcName();
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
        return "GRPC".equals(endpoint.getProtocol());
    }
}
