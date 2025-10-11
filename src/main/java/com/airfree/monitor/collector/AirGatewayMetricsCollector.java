package com.airfree.monitor.collector;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class AirGatewayMetricsCollector {

    private final MeterRegistry meterRegistry;
    // 核心指标
    private  Counter totalRequests;
    private Counter errorRequests;
    private Timer requestDuration;
    private  DistributionSummary requestSize;
    private  Gauge activeRequests;

    // 路由级别指标
    private final Map<String, Counter> routeCounters;
    private final Map<String, Timer> routeTimers;

    private AtomicInteger currentActiveRequests;

    public AirGatewayMetricsCollector() {
        //不同的MeterRegistry有不同的配置，这里先搞一个PrometheusMeterRegistry的实例
        this.meterRegistry = new PrometheusMeterRegistry(new PrometheusConfig() {
            @Override
            public String get(String key) {
                return null;
            }
        });
        this.routeCounters = new ConcurrentHashMap<>();
        this.routeTimers = new ConcurrentHashMap<>();
        this.currentActiveRequests = new AtomicInteger(0);
        initializeMetrics();
        registerJvmMetrics();
    }

    private void initializeMetrics() {
        try {
            // 使用 MeterBinder 模式，Spring 会处理重复注册问题
            this.totalRequests = Counter.builder("gateway.requests.total")
                    .description("网关总请求数")
                    .register(meterRegistry);

            this.errorRequests = Counter.builder("gateway.requests.error")
                    .description("网关错误请求数")
                    .register(meterRegistry);

            this.requestDuration = Timer.builder("gateway.request.duration")
                    .description("网关请求耗时")
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .register(meterRegistry);

            this.requestSize = DistributionSummary.builder("gateway.request.size")
                    .description("请求大小分布")
                    .baseUnit("bytes")
                    .register(meterRegistry);

            this.activeRequests = Gauge.builder("gateway.requests.active", currentActiveRequests, AtomicInteger::get)
                    .description("当前活跃请求数")
                    .register(meterRegistry);

        } catch (Exception e) {
            log.error("❌ 指标绑定失败: {}", e.getMessage(), e);
            // 不抛出异常，允许应用继续运行
        }
    }


    public void recordRequestStart(String path, String method) {
        currentActiveRequests.incrementAndGet();
        totalRequests.increment();

        // 记录路由级别指标
        String routeKey = getRouteKey(path, method);
        getRouteCounter(routeKey).increment();
    }

    public void recordRequestEnd(String path, String method, long durationMs,
                                 int statusCode, boolean isError) {
        currentActiveRequests.decrementAndGet();

        String routeKey = getRouteKey(path, method);

        // 记录耗时
        requestDuration.record(durationMs, TimeUnit.MILLISECONDS);
        getRouteTimer(routeKey).record(durationMs, TimeUnit.MILLISECONDS);

        // 记录错误
        if (isError || statusCode >= 400) {
            errorRequests.increment();

            Counter.builder("gateway.requests.error.by.status")
                    .tag("status", String.valueOf(statusCode))
                    .tag("path", getPathGroup(path))
                    .register(meterRegistry)
                    .increment();
        }

        // 记录状态码分布
        Counter.builder("gateway.requests.by.status")
                .tag("status", String.valueOf(statusCode))
                .register(meterRegistry)
                .increment();
    }

    public void recordRequestSize(String path, long sizeBytes) {
        requestSize.record(sizeBytes);
    }

    public void recordRouteLatency(String routeId, long latencyMs) {
        Timer.builder("gateway.route.latency")
                .tag("route", routeId)
                .register(meterRegistry)
                .record(latencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordBackendCall(String backend, long durationMs, boolean success) {
        Timer.builder("gateway.backend.call.duration")
                .tag("backend", backend)
                .tag("success", String.valueOf(success))
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);

        Counter.builder("gateway.backend.calls")
                .tag("backend", backend)
                .tag("success", String.valueOf(success))
                .register(meterRegistry)
                .increment();
    }

    // 缓存路由级别指标以避免重复创建
    private Counter getRouteCounter(String routeKey) {
        return routeCounters.computeIfAbsent(routeKey, key ->
                Counter.builder("gateway.requests.by.route")
                        .tag("route", key)
                        .description("按路由统计的请求数")
                        .register(meterRegistry)
        );
    }

    private Timer getRouteTimer(String routeKey) {
        return routeTimers.computeIfAbsent(routeKey, key ->
                Timer.builder("gateway.request.duration.by.route")
                        .tag("route", key)
                        .description("按路由统计的请求耗时")
                        .register(meterRegistry)
        );
    }

    private String getRouteKey(String path, String method) {
        return method + ":" + getPathGroup(path);
    }

    private String getPathGroup(String path) {
        if (path.startsWith("/api/")) return "api";
        if (path.startsWith("/admin/")) return "admin";
        if (path.startsWith("/ai/")) return "ai";
        if (path.startsWith("/websocket/")) return "websocket";
        if (path.startsWith("/smpp/")) return "smpp";
        if (path.startsWith("/backend/")) return "backend";
        return "other";
    }

    // JVM 和系统指标（可选）
    private void registerJvmMetrics() {
        // JVM 内存指标
        JvmMemoryMetrics jvmMemoryMetrics = new JvmMemoryMetrics();
        new JvmMemoryMetrics().bindTo(meterRegistry);

        // JVM GC 指标
        new JvmGcMetrics().bindTo(meterRegistry);

        // 系统 CPU 指标
        new ProcessorMetrics().bindTo(meterRegistry);

        // 日志框架指标
        new LogbackMetrics().bindTo(meterRegistry);
    }

}
