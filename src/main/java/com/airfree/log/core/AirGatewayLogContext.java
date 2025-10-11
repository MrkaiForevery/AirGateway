package com.airfree.log.core;

import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class AirGatewayLogContext {

    private static final String TRACE_ID = "X-Trace-Id";
    private static final String SPAN_ID = "X-Span-Id";
    private static final String REQUEST_ID = "X-Request-Id";

    public static Mono<ServerWebExchange> initLogContext(ServerWebExchange exchange) {
        String traceId = getOrGenerateTraceId(exchange);
        String spanId = generateSpanId();
        String requestId = generateRequestId();

        // 设置到 exchange attribute
        exchange.getAttributes().put(TRACE_ID, traceId);
        exchange.getAttributes().put(SPAN_ID, spanId);
        exchange.getAttributes().put(REQUEST_ID, requestId);

        // 设置到响应头
        exchange.getResponse().getHeaders().add(TRACE_ID, traceId);
        exchange.getResponse().getHeaders().add(SPAN_ID, spanId);

        return Mono.just(exchange);
    }

    public static String getTraceId(ServerWebExchange exchange) {
        return exchange.getAttribute(TRACE_ID);
    }

    public static String getSpanId(ServerWebExchange exchange) {
        return exchange.getAttribute(SPAN_ID);
    }

    public static String getRequestId(ServerWebExchange exchange) {
        return exchange.getAttribute(REQUEST_ID);
    }

    private static String getOrGenerateTraceId(ServerWebExchange exchange) {
        return exchange.getRequest().getHeaders().getFirst(TRACE_ID) != null
                ? exchange.getRequest().getHeaders().getFirst(TRACE_ID)
                : UUID.randomUUID().toString().replace("-", "");
    }

    private static String generateSpanId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    private static String generateRequestId() {
        return System.currentTimeMillis() + "-" +
                ThreadLocalRandom.current().nextInt(100000, 999999);
    }
}
