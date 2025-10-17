package com.airfree.rpc.entity;

import com.airfree.LoadBalance.AirGatewayLoadBalanceStrategy;
import lombok.Data;

import java.util.Map;

@Data
public class AirServiceEndpoint {
    private String serviceName;
    private String protocol; // RSOCKET, GRPC, HTTP, DUBBO
    private String endpoint; // host:port or service discovery key
    private Map<String, String> metadata;
    private AirGatewayLoadBalanceStrategy loadBalance;
}
