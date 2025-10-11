package com.airfree.log.core;

import com.airfree.entity.log.AirGatewayLog;
import com.airfree.log.config.AirGatewayLogProperties;
import com.airfree.log.core.storage.AirGatewayLogStorage;
import com.airfree.log.event.AirGatewayErrorEvent;
import com.airfree.log.event.AirGatewayLogEvent;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AirGatewayLogPublisher {

    private final AirGatewayLogStorage logStorage;
    private final AirGatewayLogProperties properties;
    private final MeterRegistry meterRegistry;
    private final ApplicationEventPublisher eventPublisher;
    private final AirGatewayJavaClock clock;

    // 异步处理日志的调度器
    private final Scheduler logScheduler;

    public AirGatewayLogPublisher(AirGatewayLogStorage logStorage,
                                  AirGatewayLogProperties properties,
                                  MeterRegistry meterRegistry,
                                  ApplicationEventPublisher eventPublisher,
                                  AirGatewayJavaClock clock) {
        this.logStorage = logStorage;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.eventPublisher = eventPublisher;
        this.logScheduler = Schedulers.newBoundedElastic(
                properties.getLogThreadPoolSize(),
                properties.getLogQueueCapacity(),
                "gateway-log"
        );
        this.clock = clock;
    }

    public void publishAccessLog(AirGatewayLog gatewayLog) {
        if (!properties.isEnableAccessLog()) {
            return;
        }

        Mono.fromRunnable(() -> {
                    try {
                        // 记录指标
                        recordMetrics(gatewayLog);

                        // 存储日志
                        logStorage.saveAccessLog(gatewayLog);

                        // 发布事件
                        eventPublisher.publishEvent(new AirGatewayLogEvent(this, this.clock, gatewayLog));

                        // 控制台输出
                        if (properties.isConsoleOutput()) {
                            log.info("[ACCESS] {} {} - {}ms - {}",
                                    gatewayLog.getMethod(),
                                    gatewayLog.getPath(),
                                    gatewayLog.getCostTime(),
                                    gatewayLog.getStatusCode());
                        }
                    } catch (Exception e) {
                        log.error("记录访问日志失败", e);
                    }
                })
                .subscribeOn(logScheduler)
                .subscribe();
    }

    public void publishErrorLog(AirGatewayLog gatewayLog) {
        Mono.fromRunnable(() -> {
                    try {
                        // 记录错误指标
                        recordErrorMetrics(gatewayLog);

                        // 存储错误日志
                        logStorage.saveErrorLog(gatewayLog);

                        // 发布错误事件
                        eventPublisher.publishEvent(new AirGatewayErrorEvent(this, this.clock, gatewayLog));

                        // 控制台输出
                        log.error("[ERROR] {} {} - {} - {}",
                                gatewayLog.getMethod(),
                                gatewayLog.getPath(),
                                gatewayLog.getStatusCode(),
                                gatewayLog.getErrorMessage());
                    } catch (Exception e) {
                        log.error("记录错误日志失败", e);
                    }
                })
                .subscribeOn(logScheduler)
                .subscribe();
    }

    public void publishBusinessLog(String traceId,String module, String action, Object data) {
        Map<String, Object> extraInfo = new HashMap<>();
        extraInfo.put("module",module);
        extraInfo.put("action",action);
        extraInfo.put("data",data);
        AirGatewayLog businessLog = AirGatewayLog.builder()
                .traceId(traceId)
                .timestamp(System.currentTimeMillis())
                .extraInfo(extraInfo)
                .build();

        publishBusinessLog(businessLog);
    }

    private void publishBusinessLog(AirGatewayLog gatewayLog) {
        Mono.fromRunnable(() -> {
                    try {
                        logStorage.saveBusinessLog(gatewayLog);
                    } catch (Exception e) {
                        log.error("记录业务日志失败", e);
                    }
                })
                .subscribeOn(logScheduler)
                .subscribe();
    }

    private void recordMetrics(AirGatewayLog gatewayLog) {
        // 记录请求计数
        meterRegistry.counter("gateway.requests.total",
                        "method", gatewayLog.getMethod(),
                        "path", gatewayLog.getPath(),
                        "status", String.valueOf(gatewayLog.getStatusCode()))
                .increment();

        // 记录请求耗时
        if (gatewayLog.getCostTime() != null) {
            meterRegistry.timer("gateway.request.duration",
                            "method", gatewayLog.getMethod(),
                            "path", gatewayLog.getPath())
                    .record(gatewayLog.getCostTime(), TimeUnit.MILLISECONDS);
        }
    }

    private void recordErrorMetrics(AirGatewayLog gatewayLog) {
        meterRegistry.counter("gateway.requests.error",
                        "method", gatewayLog.getMethod(),
                        "path", gatewayLog.getPath(),
                        "error", gatewayLog.getErrorMessage())
                .increment();
    }

    public AirGatewayJavaClock getClock() {
        return this.clock;
    }
}
