package com.airfree.monitor.endPoint;

import com.airfree.monitor.collector.AirGatewayMetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.annotation.Resource;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
//todo 这个端点是什么鬼，要搞清楚这个Spring Boot Actuator
@Endpoint(id = "custom")
public class AirGatewayCustomMetricsEndpoint {

    @Resource
    private PrometheusMeterRegistry meterRegistry;
    private final AirGatewayMetricsCollector metricsCollector;


    public AirGatewayCustomMetricsEndpoint(AirGatewayMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    //todo 这个注解是什么意思
    @ReadOperation
    public Map<String, Object> gatewayMetrics() {
        Map<String, Object> metrics = new HashMap<>();

        // 获取总请求数
        Counter totalRequests = meterRegistry.find("gateway.requests.total").counter();
        if (totalRequests != null) {
            metrics.put("totalRequests", totalRequests.count());
        }

        // 获取错误率
        Counter errorRequests = meterRegistry.find("gateway.requests.error").counter();
        if (totalRequests != null && errorRequests != null && totalRequests.count() > 0) {
            double errorRate = errorRequests.count() / totalRequests.count();
            metrics.put("errorRate", Math.round(errorRate * 10000) / 100.0);
        }

        // 获取平均响应时间
        Timer requestTimer = meterRegistry.find("gateway.request.duration").timer();
        if (requestTimer != null) {
            metrics.put("avgResponseTime", requestTimer.mean(TimeUnit.MILLISECONDS));
            metrics.put("p95ResponseTime", requestTimer.percentile(0.95, TimeUnit.MILLISECONDS));
            metrics.put("maxResponseTime", requestTimer.max(TimeUnit.MILLISECONDS));
        }

        return metrics;
    }
}
