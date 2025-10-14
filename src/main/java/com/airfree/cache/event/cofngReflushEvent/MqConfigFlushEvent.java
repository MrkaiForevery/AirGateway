package com.airfree.cache.event.cofngReflushEvent;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.cache.cacheEnums.AirGatewayCacheEventOperationEnum;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import com.airfree.mq.cofig.AbstractAirMqConfig;
import com.airfree.mq.cofig.rocketMq.AirRocketMqConfig;
import com.airfree.mq.mqEnums.MqTypeEnum;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MqConfigFlushEvent extends AirGatewayAbstractCacheEvent<String, AbstractAirMqConfig> {

    private final MqTypeEnum mqTypeEnum;
    private final boolean isAllowRefreshed;

    public MqConfigFlushEvent(Object source,
                              AirGatewayJavaClock clock,
                              AirGatewayCacheEventOperationEnum operation,
                              String airGatewayCacheKey,
                              AbstractAirMqConfig airGatewayCacheValue,
                              String reason,
                              MqTypeEnum mqTypeEnum) {
        super(source, clock, operation, airGatewayCacheKey, airGatewayCacheValue, reason);
        this.isAllowRefreshed = false;
        this.mqTypeEnum = mqTypeEnum;
    }
}
