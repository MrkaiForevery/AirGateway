package com.airfree.flow.core;


import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.lang.reflect.Constructor;
import java.util.function.Function;

@Slf4j
public class AirFlowControlOperatorFactory {

    public static <T> AirFlowControlOperator<T> getAirFlowControlOperator(AirFlowControlType flowControlType,
                                                                          AirOperatorConfig config,
                                                                          Function<Throwable, Mono<T>> fallbackFunction) throws Exception {

        Class<? extends AirFlowControlOperator> operatorClass = flowControlType.getFlowControlClass();
        return createOperatorInstance(operatorClass,flowControlType, config, fallbackFunction);
    }

    /**
     * 尝试不同的构造方法创建实例
     */
    @SuppressWarnings("unchecked")
    public static <T> AirFlowControlOperator<T> createOperatorInstance(
            Class<? extends AirFlowControlOperator> operatorClass,
            AirFlowControlType flowControlType,
            AirOperatorConfig config,
            Function<Throwable, Mono<T>> fallbackFunction)
            throws Exception {

        // todo 尝试构造方法1: (AirFlowControlType flowControlTyp, AirOperatorConfig config, Function fallback)
        try {
            log.info("尝试构造方法1：(AirFlowControlType flowControlTyp, AirOperatorConfig config, Function fallback)创建flowControlInstance....");
            Constructor<?> constructor = operatorClass.getConstructor( String.class,AirFlowControlType.class, AirOperatorConfig.class, Function.class);
            return (AirFlowControlOperator<T>) constructor.newInstance(flowControlType,config, fallbackFunction);
        } catch (NoSuchMethodException e) {
            log.info("尝试构造方法1：(AirFlowControlType flowControlTyp, AirOperatorConfig config, Function fallback)创建flowControlInstance失败！！！");
            // todo 继续尝试其他构造方法
        }

        // todo 尝试构造方法2 如果有的话
        return null;
    }
}
