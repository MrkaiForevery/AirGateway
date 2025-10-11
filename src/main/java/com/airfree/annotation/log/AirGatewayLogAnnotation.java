package com.airfree.annotation.log;

import com.airfree.log.logEnums.AirGatewayLogLevelEnum;
import com.airfree.log.logEnums.AirGatewayLogTypeEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 日志注解使用
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AirGatewayLogAnnotation {
    String value() default "";
    AirGatewayLogLevelEnum level() default AirGatewayLogLevelEnum.INFO;
    AirGatewayLogTypeEnum type() default AirGatewayLogTypeEnum.BUSINESS;
    boolean async() default true; // 是否异步执行
}
