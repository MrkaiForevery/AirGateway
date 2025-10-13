package com.airfree.cache;

import com.airfree.monitor.actuator.AirGatewayJavaClock;
import org.springframework.context.ApplicationEvent;

public abstract class AirGatewayAbstractCacheEvent extends ApplicationEvent {

    private final String airGatewayCacheName;

    public AirGatewayAbstractCacheEvent(Object source, AirGatewayJavaClock clock, String cacheName) {
        super(source, clock);
        this.airGatewayCacheName = cacheName;
    }

    public String getAirGatewayCacheName() {
        return airGatewayCacheName;
    }

}
