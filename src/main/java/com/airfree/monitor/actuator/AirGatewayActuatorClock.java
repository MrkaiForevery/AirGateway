package com.airfree.monitor.actuator;

import io.micrometer.core.instrument.Clock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AirGatewayActuatorClock implements Clock {

    @Override
    public long wallTime() {
        return System.currentTimeMillis();  // 返回当前系统时间
    }

    @Override
    public long monotonicTime() {
        return System.nanoTime();  // 返回单调递增的纳秒时间
    }
}
