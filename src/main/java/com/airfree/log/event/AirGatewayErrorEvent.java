package com.airfree.log.event;

import com.airfree.entity.log.AirGatewayLog;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import org.springframework.context.ApplicationEvent;


public class AirGatewayErrorEvent extends ApplicationEvent {

    private final AirGatewayLog gatewayLog;

    private final AirGatewayJavaClock clock;

    public AirGatewayErrorEvent(Object source, AirGatewayJavaClock clock, AirGatewayLog gatewayLog) {
        super(source,clock);
        this.clock = clock;
        this.gatewayLog = gatewayLog;
    }

    public AirGatewayLog getAirGatewayLog() {
        return gatewayLog;
    }
}
