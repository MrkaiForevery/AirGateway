package com.airfree.cache;

import com.airfree.cache.cacheEnums.AirGatewayCacheEventOperationEnum;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public abstract class AirGatewayAbstractCacheEvent<K,V> extends ApplicationEvent {

    private final AirGatewayCacheEventOperationEnum operation;
    private final K airGatewayCacheKey;
    private final V airGatewayCacheValue;
    private final String reason;

    public AirGatewayAbstractCacheEvent(Object source,
                                        AirGatewayJavaClock clock,
                                        AirGatewayCacheEventOperationEnum operation,
                                        K airGatewayCacheKey,
                                        V airGatewayCacheValue,
                                        String reason) {
        super(source, clock);
        this.operation = operation;
        this.airGatewayCacheKey = airGatewayCacheKey;
        this.airGatewayCacheValue = airGatewayCacheValue;
        this.reason = reason;
    }
}
