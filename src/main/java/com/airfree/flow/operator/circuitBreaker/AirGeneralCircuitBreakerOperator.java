package com.airfree.flow.operator.circuitBreaker;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import com.airfree.flow.enums.AirFlowRecordOrIgnoreExceptionEnum;
import com.airfree.flow.exception.AirCircuitBreakerOpenException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import javax.naming.ServiceUnavailableException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 通用熔断操作器实现类*
 *
 * @param <T>
 */
@Slf4j
public class AirGeneralCircuitBreakerOperator<T> implements AirFlowControlOperator<T> {

    private final String name;
    private final AirFlowControlType type;
    private final AirOperatorConfig config;
    private final String resource;

    //todo 这里使用resilience4j作为默认的熔断器
    private final CircuitBreaker circuitBreaker;
    private final Function<Throwable, Mono<T>> fallbackFunction;

    public AirGeneralCircuitBreakerOperator(String name,
                                            AirOperatorConfig config,
                                            Function<Throwable, Mono<T>> fallbackFunction) throws RuntimeException{
        log.info("开始构建新的AirGeneralCircuitBreakerOperator....");
        this.name = name;
        this.type = AirFlowControlType.CIRCUIT_BREAKER;
        this.config = config;
        this.resource = config.getResource();

        //todo 这里默认用Resilience4j的CircuitBreaker
        CircuitBreakerConfig circuitBreakerConfig = buildResilience4jCircuitBreakerConfig(this.config);
        this.circuitBreaker = CircuitBreaker.of(name, circuitBreakerConfig);
        this.fallbackFunction = fallbackFunction != null ? fallbackFunction : this::defaultFallback;
        log.info("新的AirGeneralCircuitBreakerOperator构建完毕:{}",this);
    }


    public CircuitBreakerConfig buildResilience4jCircuitBreakerConfig(AirOperatorConfig config) {
        Map<String, Object> configFiledMap = config.getConfig();
        // 使用构建器模式创建 CircuitBreakerConfig
        CircuitBreakerConfig.Builder configBuilder = CircuitBreakerConfig.custom();
        //todo 映射基础配置项
        //失败率阈值 eg:50
        int failureRateThreshold = Integer.parseInt(configFiledMap.get("failureRateThreshold").toString());
        configBuilder.failureRateThreshold(failureRateThreshold);
        //熔断器打开状态下的等待时长 eg:30000
        Duration waitDurationInOpenState = Duration.ofMillis(Long.parseLong(configFiledMap.get("waitDurationInOpenState").toString()));
        configBuilder.waitDurationInOpenState(waitDurationInOpenState);
        //半开状态下允许的呼叫次数 eg:50
        int permittedNumberOfCallsInHalfOpenState = Integer.parseInt(configFiledMap.get("permittedNumberOfCallsInHalfOpenState").toString());
        configBuilder.permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState);
        //滑动窗口大小,是指统计时间被拆分的份数是多少 eg: 60
        int slidingWindowSize = Integer.parseInt(configFiledMap.get("slidingWindowSize").toString());
        configBuilder.slidingWindowSize(slidingWindowSize);
        //最小通话次数 eg:10
        int minimumNumberOfCalls = Integer.parseInt(configFiledMap.get("minimumNumberOfCalls").toString());
        configBuilder.minimumNumberOfCalls(minimumNumberOfCalls);

        //todo 映射慢调用相关配置
        //慢速通话持续时间阈值 eg:6000
        if (configFiledMap.get("slowCallDurationThreshold") != null) {
            Duration slowCallDurationThreshold = Duration.ofMillis(Long.parseLong(configFiledMap.get("slowCallDurationThreshold").toString()));
            configBuilder.slowCallDurationThreshold(slowCallDurationThreshold);
        }
        //慢速呼叫率阈值 eg:0.5
        if (configFiledMap.get("slowCallRateThreshold") != null) {
            Float slowCallRateThreshold = Float.valueOf(configFiledMap.get("slowCallRateThreshold").toString());
            configBuilder.slowCallRateThreshold(slowCallRateThreshold);
        }

        //todo 设置滑动窗口类型
        if (configFiledMap.get("slidingWindowType") != null && "TIME_BASED".equals(configFiledMap.get("slidingWindowType").toString())) {
            configBuilder.slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED);
        } else {
            // 默认使用 COUNT_BASED
            configBuilder.slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED);
        }

        // todo 映射异常记录与忽略配置
        String[] splitRecordString = configFiledMap.get("recordExceptions").toString().split(",");
        if (splitRecordString.length > 0) {
            List<Class<? extends Throwable>> recordExceptions = findMatchExceptions(splitRecordString, "recordException");
            if (recordExceptions != null && !recordExceptions.isEmpty()) {
                configBuilder.recordExceptions(recordExceptions.toArray(new Class[0]));
            }
        }
        String[] splitIgnoreString = configFiledMap.get("ignoreExceptions").toString().split(",");
        if (splitIgnoreString.length > 0) {
            List<Class<? extends Throwable>> ignoreExceptions = findMatchExceptions(splitRecordString, "ignoreException");
            if (ignoreExceptions != null && !ignoreExceptions.isEmpty()) {
                configBuilder.ignoreExceptions(ignoreExceptions.toArray(new Class[0]));
            }
        }
        return configBuilder.build();
    }

    private List<Class<? extends Throwable>> findMatchExceptions(String[] splitString, String eType) {
        List<Class<? extends Throwable>> exceptionList = new ArrayList<>();
        for (int i = 0; i < splitString.length; i++) {
            AirFlowRecordOrIgnoreExceptionEnum recordException = AirFlowRecordOrIgnoreExceptionEnum.getByFlowTypeAndName(eType, splitString[i]);
            if (recordException != null) {
                exceptionList.add(recordException.getExceptionClass());
            }
        }
        return exceptionList;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public AirFlowControlType getType() {
        return this.type;
    }

    @Override
    public Mono<T> apply(Mono<T> sourceMono) {
        // 检查熔断器状态
        if (!circuitBreaker.tryAcquirePermission()) {
            return fallbackFunction.apply(new AirCircuitBreakerOpenException(
                    "Circuit breaker '" + name + "' is open"));
        }
        final long start = System.nanoTime();
        return sourceMono
                .doOnSuccess(result -> {
                    circuitBreaker.onSuccess(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                })
                .doOnError(error -> {
                    circuitBreaker.onError(System.nanoTime() - start, TimeUnit.NANOSECONDS, error);
                })
                .onErrorResume(error -> fallbackFunction.apply(error));
    }

    /**
     * 默认降级方法
     */
    private Mono<T> defaultFallback(Throwable error) {
        if (error instanceof AirCircuitBreakerOpenException) {
            return Mono.error(new ServiceUnavailableException(
                    "Service unavailable due to circuit breaker: " + name));
        }
        return Mono.error(error);
    }

    // 其他熔断器相关方法
    private CircuitBreaker.State checkCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    /**
     * 获取熔断器指标
     */
    public CircuitBreaker.Metrics getMetrics() {
        return circuitBreaker.getMetrics();
    }

    /**
     * 重置熔断器
     */
    public void reset() {
        circuitBreaker.reset();
    }
}
