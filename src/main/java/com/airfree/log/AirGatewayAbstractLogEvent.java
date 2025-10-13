package com.airfree.log;

import org.springframework.context.ApplicationEvent;

import java.time.Clock;

public abstract class AirGatewayAbstractLogEvent extends ApplicationEvent {

    public AirGatewayAbstractLogEvent(Object source) {
        super(source);
    }

    public AirGatewayAbstractLogEvent(Object source, Clock clock) {
        super(source, clock);
    }

}
