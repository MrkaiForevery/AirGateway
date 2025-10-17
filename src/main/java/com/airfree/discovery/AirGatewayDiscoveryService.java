package com.airfree.discovery;

import com.airfree.rpc.entity.AirServiceEndpoint;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AirGatewayDiscoveryService  {

    Mono<AirServiceEndpoint> resolveEndpoint(String serviceName, String protocol);

    String getDiscoveryName();

    List<AirRPCTypeEnum> supportRpcType();
}
