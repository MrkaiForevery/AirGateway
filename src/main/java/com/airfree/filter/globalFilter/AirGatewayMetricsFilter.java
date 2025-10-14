package com.airfree.filter.globalFilter;

import com.airfree.monitor.collector.AirGatewayMetricsCollector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Slf4j
@Component
// 在日志过滤器之前执行,这里指的是过滤器执行的顺序，不是加载的顺序
@Order(Ordered.HIGHEST_PRECEDENCE - 2)
public class AirGatewayMetricsFilter implements WebFilter {

    private final AirGatewayMetricsCollector metricsCollector;

    public AirGatewayMetricsFilter(AirGatewayMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        log.info("AirGatewayMetricsFilter过滤器初始化完成");
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        log.info("执行AirGatewayMetricsFilter过滤器");
        long startTime = System.currentTimeMillis();
        String path = exchange.getRequest().getPath().value();
        String method = exchange.getRequest().getMethod().name();

        // 记录请求开始
        metricsCollector.recordRequestStart(path, method);

        // 记录请求大小（如果有body）
        if (exchange.getRequest().getHeaders().getContentLength() > 0) {
            metricsCollector.recordRequestSize(path,
                    exchange.getRequest().getHeaders().getContentLength());
        }

        return chain.filter(exchange)
                .doOnSuccess(v -> {
                    long duration = System.currentTimeMillis() - startTime;
                    int statusCode = exchange.getResponse().getStatusCode() != null
                            ? exchange.getResponse().getStatusCode().value()
                            : 200;

                    boolean isError = statusCode >= 400;
                    metricsCollector.recordRequestEnd(path, method, duration, statusCode, isError);
                })
                .doOnError(throwable -> {
                    long duration = System.currentTimeMillis() - startTime;
                    metricsCollector.recordRequestEnd(path, method, duration, 500, true);
                });
    }
}
