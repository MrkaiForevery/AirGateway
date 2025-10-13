package com.airfree.log.event;

import com.airfree.entity.log.AirGatewayLog;
import com.airfree.log.AirGatewayAbstractLogEvent;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import org.springframework.context.ApplicationEvent;

public class AirGatewayLogEvent extends AirGatewayAbstractLogEvent {

    private final AirGatewayLog gatewayLog;
    private final AirGatewayJavaClock clock ;

    public AirGatewayLogEvent(Object source, AirGatewayJavaClock clock, AirGatewayLog gatewayLog) {
        super(source,clock);
        this.gatewayLog = gatewayLog;
        this.clock = clock;
    }

    public AirGatewayLog getGatewayLog() {
        return gatewayLog;
    }
}
