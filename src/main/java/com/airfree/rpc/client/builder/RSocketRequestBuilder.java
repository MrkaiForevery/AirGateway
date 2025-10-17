package com.airfree.rpc.client.builder;

import io.rsocket.loadbalance.LoadbalanceStrategy;
import io.rsocket.loadbalance.LoadbalanceTarget;
import io.rsocket.transport.ClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.messaging.rsocket.RSocketConnectorConfigurer;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeType;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
@Component
public class RSocketRequestBuilder implements RSocketRequester.Builder{

    @Override
    public RSocketRequester.Builder dataMimeType(MimeType mimeType) {
        return null;
    }

    @Override
    public RSocketRequester.Builder metadataMimeType(MimeType mimeType) {
        return null;
    }

    @Override
    public RSocketRequester.Builder setupData(Object data) {
        return null;
    }

    @Override
    public RSocketRequester.Builder setupRoute(String route, Object... routeVars) {
        return null;
    }

    @Override
    public RSocketRequester.Builder setupMetadata(Object value, MimeType mimeType) {
        return null;
    }

    @Override
    public RSocketRequester.Builder rsocketStrategies(RSocketStrategies strategies) {
        return null;
    }

    @Override
    public RSocketRequester.Builder rsocketStrategies(Consumer<RSocketStrategies.Builder> configurer) {
        return null;
    }

    @Override
    public RSocketRequester.Builder rsocketConnector(RSocketConnectorConfigurer configurer) {
        return null;
    }

    @Override
    public RSocketRequester.Builder apply(Consumer<RSocketRequester.Builder> configurer) {
        return null;
    }

    @Override
    public RSocketRequester tcp(String host, int port) {
        return null;
    }

    @Override
    public RSocketRequester websocket(URI uri) {
        return null;
    }

    @Override
    public RSocketRequester transport(ClientTransport transport) {
        return null;
    }

    @Override
    public RSocketRequester transports(Publisher<List<LoadbalanceTarget>> targetPublisher, LoadbalanceStrategy loadbalanceStrategy) {
        return null;
    }

    @Override
    public Mono<RSocketRequester> connectTcp(String host, int port) {
        return null;
    }

    @Override
    public Mono<RSocketRequester> connectWebSocket(URI uri) {
        return null;
    }

    @Override
    public Mono<RSocketRequester> connect(ClientTransport transport) {
        return null;
    }
}
