package com.airfree.monitor.actuator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

@Slf4j
@Component
public class AirGatewayJavaClock  extends java.time.Clock{

    private final ZoneId zone;

    public AirGatewayJavaClock() {
        this.zone = ZoneId.systemDefault();  // 使用系统默认时区
    }

    public AirGatewayJavaClock(ZoneId zone) {
        this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new AirGatewayJavaClock(zone);
    }

    @Override
    public Instant instant() {
        return Instant.now();  // 返回当前时刻
    }
}
