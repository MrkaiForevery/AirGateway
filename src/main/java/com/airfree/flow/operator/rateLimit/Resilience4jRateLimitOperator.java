package com.airfree.flow.operator.rateLimit;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import com.airfree.flow.exception.AirRateLimitExceededException;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class Resilience4jRateLimitOperator<T> implements AirFlowControlOperator<T> {

    private final AirFlowControlType type;
    private final AirOperatorConfig config;
    private final String resource;

    //todo 这里使用resilience4j作为x限流器
    private final RateLimiter rateLimiter;
    private final Function<Throwable, Mono<T>> fallbackFunction;

    public Resilience4jRateLimitOperator(AirFlowControlType flowType,
                                         AirOperatorConfig config,
                                         Function<Throwable, Mono<T>> fallbackFunction) {
        log.info("开始构建新的Resilience4jRateLimitOperator....");
        this.type = flowType;
        this.config = config;
        this.resource = config.getResource();

        this.rateLimiter = RateLimiter.of(flowType.getFlowTypeName() + "_" + flowType.getAlgorithm(), buildResilience4jRateLimiterConfig(config));
        this.fallbackFunction = fallbackFunction != null ? fallbackFunction : this::defaultFallback;
        log.info("构建新的Resilience4jRateLimitOperator成功");
    }

    private RateLimiterConfig buildResilience4jRateLimiterConfig(AirOperatorConfig config) {
        RateLimiterConfig.Builder rateLimiterConfigBuilder = RateLimiterConfig.custom();
        Map<String, Object> configMap = config.getConfig();

        //todo 参数设置
        //在一个刷新周期内允许的最大请求数量 eg:50
        int limitForPeriod = Integer.parseInt(configMap.get("limitForPeriod").toString());
        rateLimiterConfigBuilder.limitForPeriod(limitForPeriod);
        //重置计数器的时间周期: eg:5000 ---纳秒
        Duration limitRefreshPeriod = Duration.ofNanos(Long.parseLong(configMap.get("limitRefreshPeriod").toString()));
        rateLimiterConfigBuilder.limitRefreshPeriod(limitRefreshPeriod);
        //线程等待许可的最长等待时间: eg:5 ---秒
        Duration timeoutDuration = Duration.ofSeconds(Long.parseLong(configMap.get("timeoutDuration").toString()));
        rateLimiterConfigBuilder.timeoutDuration(timeoutDuration);
//        //根据结果设置权限 todo 这个参数没搞懂，先注释掉
//        rateLimiterConfigBuilder.drainPermissionsOnResult();
        //是否启用可写堆栈跟踪
        boolean writableStackTraceEnabled = Boolean.valueOf(configMap.get("writableStackTraceEnabled").toString());
        rateLimiterConfigBuilder.writableStackTraceEnabled(writableStackTraceEnabled);

        return rateLimiterConfigBuilder.build();
    }

    @Override
    public String getName() {
        return this.type.getFlowTypeName() + "_" +  this.type.getAlgorithm();
    }

    @Override
    public AirFlowControlType getType() {
        return this.type;
    }

    @Override
    public Mono<T> apply(Mono<T> tMono) {
        return null;
    }

    /**
     * 默认降级方法
     */
    private Mono<T> defaultFallback(Throwable throwable) {
        return Mono.error(new AirRateLimitExceededException(
                "Rate limit exceeded for resource: " + this.type.getFlowTypeName() + "_" + this.type.getAlgorithm()));
    }
}
