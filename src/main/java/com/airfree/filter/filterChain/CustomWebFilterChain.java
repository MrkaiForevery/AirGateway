package com.airfree.filter.filterChain;

import com.airfree.filter.AbstractAirGatewayFilter;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 ** 使用此类时需要对于每一个请求都要new一个实例出来，保证线程安全
 */
public class CustomWebFilterChain implements WebFilterChain {

    private List<AbstractAirGatewayFilter> filters;
    private WebFilterChain originalFilterChain;
    private final AtomicInteger index;

    public CustomWebFilterChain(List<AbstractAirGatewayFilter> filters, WebFilterChain originalChain) {
        // 防御性拷贝
        this.filters = Collections.unmodifiableList(new CopyOnWriteArrayList<>(filters));
        this.originalFilterChain = originalChain;
        // 每次新实例都从0开始
        this.index = new AtomicInteger(0);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange) {
        int currentIndex = index.getAndIncrement();
        if (this.index.get() < filters.size()) {
            // 获取当前过滤器并递增索引
            AbstractAirGatewayFilter currentFilter = filters.get(currentIndex);
            // 调用当前过滤器，并传入 this（即链中的下一个节点）
            return currentFilter.filter(exchange, this);
        } else {
            // 所有自定义过滤器执行完毕，执行原始链
            return originalFilterChain.filter(exchange);
        }
    }
}
