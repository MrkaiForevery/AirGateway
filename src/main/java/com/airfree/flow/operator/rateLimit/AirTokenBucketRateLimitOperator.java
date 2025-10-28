package com.airfree.flow.operator.rateLimit;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import com.airfree.flow.exception.AirRateLimitExceededException;
import com.google.common.util.concurrent.AtomicDouble;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 这里都是简单的自定义实现，提供架构设计参考价值*
 * @param <T>
 */
public class AirTokenBucketRateLimitOperator<T> implements AirFlowControlOperator<T> {

    private final AirFlowControlType type;
    private final AirOperatorConfig config;
    private final String resource;

    private int capacity = 100;
    private int refillRate = 100;
    private AtomicReference<Double> tokens;
    private AtomicLong lastRefillTime;

    private final Function<Throwable, Mono<T>> fallbackFunction;

    public AirTokenBucketRateLimitOperator(AirFlowControlType airFlowControlType,
                                           AirOperatorConfig config,
                                           Function<Throwable, Mono<T>> fallbackFunction) {
        this.type = airFlowControlType;
        this.config = config;
        this.resource = config.getResource();
        buildTokenBucketRateLimitOperatorConfig(this.config);
        this.fallbackFunction = fallbackFunction != null ? fallbackFunction : this::defaultFallback;
    }

    private void buildTokenBucketRateLimitOperatorConfig(AirOperatorConfig config) {
        Map<String, Object> configMap = config.getConfig();
        // 桶容量
        int capacityFiled = Integer.parseInt(configMap.get("capacity").toString());
        this.capacity = capacityFiled;
        // 每秒补充的令牌数
        int refillRateFiled = Integer.parseInt(configMap.get("refillRate").toString());
        this.refillRate = refillRateFiled;
        // 当前令牌数
        this.tokens = new AtomicReference<>((double)this.capacity);
        // 最后补充时间
        this.lastRefillTime = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    public String getName() {
        return this.type.getFlowTypeName() + "_" + this.type.getAlgorithm();
    }

    @Override
    public AirFlowControlType getType() {
        return this.type;
    }

    @Override
    public Mono<T> apply(Mono<T> sourceMono) {
        return Mono.fromCallable(this::tryAcquire)
                .flatMap(acquired -> {
                    if (acquired) {
                        return sourceMono;
                    } else {
                        return Mono.error(new AirRateLimitExceededException(
                                "Token bucket rate limit exceeded for: " + this.getName()));
                    }
                });
    }

    /**
     * 尝试获取令牌
     */
    private boolean tryAcquire() {
        refillTokens();

        while (true) {
            double currentTokens = tokens.get();
            if (currentTokens < 1) {
                return false;
            }

            if (tokens.compareAndSet(currentTokens, currentTokens - 1)) {
                return true;
            }
        }
    }

    /**
     * 补充令牌
     */
    private void refillTokens() {
        long currentTime = System.currentTimeMillis();
        long lastTime = lastRefillTime.get();
        long timePassed = currentTime - lastTime;

        if (timePassed > 1000) { // 至少1秒才补充
            if (lastRefillTime.compareAndSet(lastTime, currentTime)) {
                double tokensToAdd = (timePassed / 1000.0) * refillRate;
                tokens.updateAndGet(current -> Math.min(capacity, current + tokensToAdd));
            }
        }
    }

    /**
     * 默认降级方法
     */
    private Mono<T> defaultFallback(Throwable throwable) {
        return Mono.error(new AirRateLimitExceededException(
                "Rate limit exceeded for resource: " + this.type.getFlowTypeName() + "_" + this.type.getAlgorithm()));
    }
}