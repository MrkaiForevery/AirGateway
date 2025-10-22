package com.airfree.flow.enums;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.operator.circuitBreaker.Resilience4jCircuitBreakerOperator;
import com.airfree.flow.operator.rateLimit.AirSlidingWindowRateLimitOperator;
import com.airfree.flow.operator.rateLimit.AirTokenBucketRateLimitOperator;
import com.airfree.flow.operator.rateLimit.Resilience4jRateLimitOperator;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

/**
 * 流控操作的类型枚举类*
 */
@Getter
@AllArgsConstructor
public enum AirFlowControlType {

    RATE_LIMIT_Resilience4j("rateLimit","Resilience4j", Resilience4jRateLimitOperator.class),
    RATE_LIMIT_TokenBucket("rateLimit","TokenBucket", AirTokenBucketRateLimitOperator.class),
    RATE_LIMIT_SlidingWindow("rateLimit","SlidingWindow", AirSlidingWindowRateLimitOperator.class),
    CIRCUIT_BREAKER_Resilience4j("circuitBreaker","Resilience4j",Resilience4jCircuitBreakerOperator.class),
    DEGRADE_ON_CONDITION_Resilience4j("degradeOnCondition","Resilience4j",Resilience4jCircuitBreakerOperator.class);

    private String flowTypeName;
    private String algorithm;
    private Class< ? extends AirFlowControlOperator> flowControlClass;

    public static boolean contains(String flowTypeName,String algorithm) {

        if (flowTypeName == null || algorithm == null) {
            return false;
        }
        return Arrays.stream(values())
                .anyMatch(e -> e.getFlowTypeName().equals(flowTypeName) &&
                        e.algorithm.equals(algorithm));
    }

    public static AirFlowControlType getByFlowTypeAndAlgorithm(String flowTypeName,String algorithm) {

        return Arrays.stream(values())
                .filter(type -> type.getFlowTypeName().equals(flowTypeName))
                .filter(ele -> ele.getAlgorithm().equals(algorithm))
                .findFirst()
                .orElse(null);
    }
}
