package com.airfree.core;

import com.airfree.entity.request.AirGatewayInternalRequest;
import com.airfree.entity.response.AirGatewayInternalResponse;
import org.springframework.web.reactive.function.server.ServerRequest;
import reactor.core.publisher.Mono;

public interface AirGateway {

    Mono<AirGatewayInternalResponse> routeApiRequest(ServerRequest serverRequest);

    Mono<AirGatewayInternalResponse> routeAdminRequest(AirGatewayInternalRequest request);

    Mono<AirGatewayInternalResponse> routeAiRequest(AirGatewayInternalRequest request);

    Mono<AirGatewayInternalResponse> routeWebsocketRequest(AirGatewayInternalRequest request);

    Mono<AirGatewayInternalResponse> routeSmppRequest(AirGatewayInternalRequest request);

    Mono<AirGatewayInternalResponse> forwardToBackend(AirGatewayInternalRequest request);

}
