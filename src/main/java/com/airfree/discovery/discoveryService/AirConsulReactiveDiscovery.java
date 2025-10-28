package com.airfree.discovery.discoveryService;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public class AirConsulReactiveDiscovery implements AirReactiveServiceDiscovery {

    @Override
    public String getRegistryType() {
        return null;
    }

    @Override
    public Mono<Void> initialize(AirRegistryCenterConfig config) {
        return null;
    }

    @Override
    public Flux<String> getServiceNames() {
        return null;
    }

    @Override
    public Flux<AirServiceInstance> getInstances(String serviceId) {
        return null;
    }

    @Override
    public Flux<AirServiceInstanceEvent> subscribe(String serviceId) {
        return null;
    }

    @Override
    public Mono<Boolean> isHealthy() {
        return null;
    }

    @Override
    public Mono<Boolean> registerInstance(String serviceName, String host, int port, Map<String, String> metadata) {
        return null;
    }

    @Override
    public Mono<Boolean> deregisterInstance(String serviceName, String host, int port) {
        return null;
    }

    @Override
    public boolean supportsRegistration() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
