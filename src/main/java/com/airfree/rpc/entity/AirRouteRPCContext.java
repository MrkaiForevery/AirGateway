package com.airfree.rpc.entity;

import com.airfree.rpc.AirGatewayRPCServiceClient;
import org.springframework.cloud.gateway.route.RouteDefinition;

public class AirRouteRPCContext {

    private final RouteDefinition route;
    private final AirGatewayRPCServiceClient client;
    private final String protocol;

    public AirRouteRPCContext(RouteDefinition route,
                           AirGatewayRPCServiceClient client,
                           String protocol) {
        this.route = route;
        this.client = client;
        this.protocol = protocol;
    }

    // getters
    public RouteDefinition getRoute() { return route; }
    public AirGatewayRPCServiceClient getClient() { return client; }
    public String getProtocol() { return protocol; }
}
