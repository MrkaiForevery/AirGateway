package com.airfree.flow.operator.rateLimit;


import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import com.airfree.flow.exception.AirRateLimitExceededException;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;

/**
 * 这里都是简单的自定义实现，提供架构设计参考价值* *
 * @param <T>
 */
public class AirSlidingWindowRateLimitOperator<T> implements AirFlowControlOperator<T> {

    private final AirFlowControlType type;
    private final AirOperatorConfig config;
    private final String resource;

    private int windowSize;
    private int maxRequests;
    private LinkedList<Long> requests;

    private final Function<Throwable, Mono<T>> fallbackFunction;

    public AirSlidingWindowRateLimitOperator(AirFlowControlType airFlowControlType,
                                             AirOperatorConfig config,
                                             Function<Throwable, Mono<T>> fallbackFunction) {
        this.type = airFlowControlType;
        this.config = config;
        this.resource = config.getResource();

        this.windowSize = 60;
        this.maxRequests = 5000;
        this.fallbackFunction = fallbackFunction != null ? fallbackFunction : this::defaultFallback;
    }

    private void buildSlidingWindowRateLimitOperatorConfig(AirOperatorConfig config) {

        Map<String, Object> configMap = config.getConfig();
        // 滑动窗口大小 ---秒
        int windowSizeFiled = Integer.parseInt(configMap.get("windowSize").toString());
        this.windowSize = windowSizeFiled;
        // 最大请求数量
        int maxRequestsFiled = Integer.parseInt(configMap.get("maxRequests").toString());
        this.maxRequests = maxRequestsFiled;
        this.requests = new LinkedList<>();
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
                                "Sliding window rate limit exceeded for: " + this.getName()));
                    }
                });
    }

    /**
     * 尝试获取许可
     */
    private synchronized boolean tryAcquire() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - (windowSize * 1000L);

        // 移除过期请求
        while (!requests.isEmpty() && requests.peekFirst() < windowStart) {
            requests.pollFirst();
        }

        // 检查是否超过限制
        if (requests.size() < maxRequests) {
            requests.addLast(currentTime);
            return true;
        }

        return false;
    }

    /**
     * 获取当前窗口请求数
     */
    public synchronized int getCurrentRequests() {
        long windowStart = System.currentTimeMillis() - (windowSize * 1000L);
        while (!requests.isEmpty() && requests.peekFirst() < windowStart) {
            requests.pollFirst();
        }
        return requests.size();
    }


    /**
     * 默认降级方法
     */
    private Mono<T> defaultFallback(Throwable throwable) {
        return Mono.error(new AirRateLimitExceededException(
                "Rate limit exceeded for resource: " + this.type.getFlowTypeName() + "_" + this.type.getAlgorithm()));
    }

}
