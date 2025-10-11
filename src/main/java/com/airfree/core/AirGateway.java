package com.airfree.core;

import com.airfree.entity.request.AirGatewayInternalRequest;
import com.airfree.entity.response.AirGatewayInternalResponse;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

public interface AirGateway {

    Mono<ServerResponse> routeApiRequest(ServerRequest serverRequest);

    Mono<ServerResponse> routeAdminRequest(ServerRequest serverRequest);

    Mono<ServerResponse> routeAiRequest(ServerRequest serverRequest);

    Mono<ServerResponse> routeWebsocketRequest(ServerRequest serverRequest);

    Mono<ServerResponse> routeSmppRequest(ServerRequest serverRequest);

    Mono<ServerResponse> forwardToBackend(ServerRequest serverRequest);

}
