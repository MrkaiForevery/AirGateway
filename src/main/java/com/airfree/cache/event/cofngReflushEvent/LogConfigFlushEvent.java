package com.airfree.cache.event.cofngReflushEvent;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.monitor.actuator.AirGatewayJavaClock;

public class LogConfigFlushEvent extends AirGatewayAbstractCacheEvent {

    public LogConfigFlushEvent(Object source, AirGatewayJavaClock clock, String cacheName) {
        super(source, clock, cacheName);
    }
}
