package com.airfree.filter.globalFilter;

import com.airfree.entity.log.AirGatewayLog;
import com.airfree.log.config.AirGatewayLogProperties;
import com.airfree.log.core.AirGatewayLogContext;
import com.airfree.log.core.AirGatewayLogPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * 应用于全局的过滤器，不走非自定义过滤器管理那套逻辑
 * 位置：在Web层的最外层，处理HTTP请求和响应的最初和最后阶段。
 * 关注点：主要关注HTTP请求和响应的基本信息，如URL、HTTP方法、状态码、请求头、响应时间等。
 * 职责：记录访问日志（Access Log），记录每一个请求的入口和出口，用于监控请求流量、性能、错误等。
 */

@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE - 1)
public class AirGatewayLogFilter implements WebFilter {

    private final AirGatewayLogPublisher logPublisher;
    private final AirGatewayLogProperties properties;

    public AirGatewayLogFilter(AirGatewayLogPublisher logPublisher, AirGatewayLogProperties properties) {
        this.logPublisher = logPublisher;
        this.properties = properties;
        log.info("AirGatewayLogFilter过滤器初始化完成");
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        log.info("执行AirGatewayLogFilter过滤器");
        long startTime = System.currentTimeMillis();
        // 初始化日志上下文
        return AirGatewayLogContext.initLogContext(exchange)
                .then(Mono.defer(() -> {
                    // 记录请求日志
                    logRequest(exchange);

                    // 继续处理链
                    return chain.filter(exchange)
                            .doOnSuccess(v -> {
                                // 记录成功响应日志
                                logResponse(exchange, startTime, null);
                            })
                            .doOnError(throwable -> {
                                // 记录错误响应日志
                                logResponse(exchange, startTime, throwable);
                            });
                }));
    }

    private void logRequest(ServerWebExchange exchange) {
        if (!properties.isEnableRequestLog()) {
            return;
        }

        ServerHttpRequest request = exchange.getRequest();
        AirGatewayLog gatewayLog = AirGatewayLog.builder()
                .traceId(AirGatewayLogContext.getTraceId(exchange))
                .spanId(AirGatewayLogContext.getSpanId(exchange))
                .requestId(AirGatewayLogContext.getRequestId(exchange))
                .method(request.getMethod().name())
                .path(request.getPath().value())
                .queryParams(request.getURI().getQuery())
                .headers(extractHeaders(request.getHeaders()))
                .clientIp(getClientIp(request))
                .requestTime(System.currentTimeMillis())
                .timestamp(System.currentTimeMillis())
                .build();

        logPublisher.publishAccessLog(gatewayLog);
    }

    private void logResponse(ServerWebExchange exchange, long startTime, Throwable throwable) {
        if (!properties.isEnableResponseLog()) {
            return;
        }

        ServerHttpResponse response = exchange.getResponse();
        AirGatewayLog gatewayLog = AirGatewayLog.builder()
                .traceId(AirGatewayLogContext.getTraceId(exchange))
                .spanId(AirGatewayLogContext.getSpanId(exchange))
                .requestId(AirGatewayLogContext.getRequestId(exchange))
                .method(exchange.getRequest().getMethod().name())
                .path(exchange.getRequest().getPath().value())
                .statusCode(response.getStatusCode() != null ? response.getStatusCode().value() : null)
                .requestTime(startTime)
                .responseTime(System.currentTimeMillis())
                .costTime(System.currentTimeMillis() - startTime)
                .timestamp(System.currentTimeMillis())
                .build();

        if (throwable != null) {
            gatewayLog.setErrorMessage(throwable.getMessage());
            gatewayLog.setErrorStack(getStackTrace(throwable));
            logPublisher.publishErrorLog(gatewayLog);
        } else {
            logPublisher.publishAccessLog(gatewayLog);
        }
    }

    private Map<String, String> extractHeaders(HttpHeaders headers) {
        Map<String, String> headerMap = new HashMap<>();
        headers.forEach((key, values) -> {
            if (shouldLogHeader(key)) {
                headerMap.put(key, String.join(",", values));
            }
        });
        return headerMap;
    }

    private boolean shouldLogHeader(String headerName) {
        return properties.getLogHeaders().stream()
                .anyMatch(pattern -> headerName.matches(pattern));
    }

    private String getClientIp(ServerHttpRequest request) {
        String xForwardedFor = request.getHeaders().getFirst("X-Forwarded-For");
        if (StringUtils.hasText(xForwardedFor)) {
            return xForwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddress() != null
                ? request.getRemoteAddress().getAddress().getHostAddress()
                : "unknown";
    }

    private String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }


}
