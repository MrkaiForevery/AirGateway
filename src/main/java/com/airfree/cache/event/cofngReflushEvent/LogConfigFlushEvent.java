package com.airfree.cache.event.cofngReflushEvent;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.cache.cacheEnums.AirGatewayCacheEventOperationEnum;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogConfigFlushEvent extends AirGatewayAbstractCacheEvent {

    public LogConfigFlushEvent(Object source, AirGatewayJavaClock clock, AirGatewayCacheEventOperationEnum operation, Object airGatewayCacheKey, Object airGatewayCacheValue, String reason) {
        super(source, clock, operation, airGatewayCacheKey, airGatewayCacheValue, reason);
    }
}
