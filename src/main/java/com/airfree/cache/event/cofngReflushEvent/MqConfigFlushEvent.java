package com.airfree.cache.event.cofngReflushEvent;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.cache.AirGatewayRefreshType;
import com.airfree.monitor.actuator.AirGatewayJavaClock;

public class MqConfigFlushEvent extends AirGatewayAbstractCacheEvent {

    private final AirGatewayRefreshType refreshType;
    private final Object key;
    private final String reason;

    public MqConfigFlushEvent(Object source,  AirGatewayJavaClock clock, String cacheName, AirGatewayRefreshType refreshType, Object key,String reason) {
        super(source, clock, cacheName);
        this.refreshType = refreshType;
        this.key = key;
        this.reason = reason;
    }

    public AirGatewayRefreshType getRefreshType() { return refreshType; }
    public Object getKey() { return key; }
    public String getReason() { return reason; }
}
