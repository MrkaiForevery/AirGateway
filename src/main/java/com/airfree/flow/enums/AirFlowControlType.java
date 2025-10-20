package com.airfree.flow.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

//流控操作的类型枚举类
@Getter
@AllArgsConstructor
public enum AirFlowControlType {

    RATE_LIMIT("rateLimit"),
    CIRCUIT_BREAKER("circuitBreaker"),
    DEGRADE_ON_CONDITION("degradeOnCondition");

    private String flowTypeName;

    public static boolean contains(String flowTypeName) {
        return Arrays.stream(values())
                .map(AirFlowControlType::getFlowTypeName)
                .anyMatch(name -> name.equals(flowTypeName));
    }

    public static AirFlowControlType getByFlowTypeName(String flowTypeName) {
        return Arrays.stream(values())
                .filter(type -> type.getFlowTypeName().equals(flowTypeName))
                .findFirst()
                .orElse(null);
    }
}
