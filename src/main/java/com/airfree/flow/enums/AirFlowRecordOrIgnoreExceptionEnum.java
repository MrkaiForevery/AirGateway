package com.airfree.flow.enums;

import com.airfree.flow.exception.AirCircuitBreakerOpenException;
import com.airfree.flow.exception.AirServiceUnavailableException;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum AirFlowRecordOrIgnoreExceptionEnum {
    AirCircuitBreakerOpen_Exception("recordException", "AirCircuitBreakerOpenException", AirCircuitBreakerOpenException.class),
    AirServiceUnavailable_Exception("ignoreException", "AirServiceUnavailableException", AirServiceUnavailableException.class);

    private String exceptionType;
    private String exceptionClassName;
    private Class<? extends Throwable> exceptionClass;

    public static AirFlowRecordOrIgnoreExceptionEnum getByFlowTypeAndName(String type, String className) {
        return Arrays.stream(values())
                .filter(e -> e.getExceptionType().equals(type) &&
                        e.getExceptionClassName().equals(className))
                .findFirst()
                .orElse(null);
    }
}
