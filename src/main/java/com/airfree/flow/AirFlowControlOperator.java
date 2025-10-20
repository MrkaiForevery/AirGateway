package com.airfree.flow;

import com.airfree.flow.enums.AirFlowControlType;
import reactor.core.publisher.Mono;

import java.util.function.Function;

//流控操作符接口---扩展接口
public interface AirFlowControlOperator<T> extends Function<Mono<T>, Mono<T>> {

    String getName();
    AirFlowControlType getType();
}
