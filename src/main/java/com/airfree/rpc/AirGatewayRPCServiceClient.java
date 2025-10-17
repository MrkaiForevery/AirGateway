package com.airfree.rpc;

import com.airfree.rpc.entity.AirRPCResponse;
import com.airfree.rpc.entity.AirRpcRequestContext;
import com.airfree.rpc.entity.AirServiceEndpoint;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AirGatewayRPCServiceClient {

    String getProtocol();

    Mono<AirRPCResponse<String>> execute(AirRpcRequestContext context);

    Flux<AirRPCResponse<String>> executeStream(AirRpcRequestContext context);

    boolean support(AirServiceEndpoint endpoint);
}
