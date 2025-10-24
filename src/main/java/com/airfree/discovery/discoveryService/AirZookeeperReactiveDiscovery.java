package com.airfree.discovery.discoveryService;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AirZookeeperReactiveDiscovery implements AirReactiveServiceDiscovery {

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
}
