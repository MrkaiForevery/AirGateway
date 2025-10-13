package com.airfree.log;

import com.airfree.log.event.AirGatewayLogEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AirGatewayLogListenerTestTempImpl implements AirGatewayLogListener<AirGatewayLogEvent>{
    @Override
    public void onLogRecordFish(AirGatewayLogEvent event) {

    }

    @Override
    public void onLogRecordError(AirGatewayLogEvent event) {

    }

    @Override
    public boolean supports(String cacheName) {
        return AirGatewayLogListener.super.supports(cacheName);
    }

    @Override
    public int getOrder() {
        return AirGatewayLogListener.super.getOrder();
    }
}
