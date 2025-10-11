package com.airfree.entity.log;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AirGatewayLog {
    // 基础信息
    private String traceId;
    private String spanId;
    private String requestId;

    // 请求信息
    private String method;
    private String path;
    private String queryParams;
    private Map<String, String> headers;
    private String clientIp;
    private Long requestTime;

    // 响应信息
    private Integer statusCode;
    private Long responseTime;
    private Long costTime;
    private String responseBody;

    // 业务信息
    private String serviceId;
    private String routeId;
    private String targetUrl;
    private String errorMessage;
    private String errorStack;

    // 系统信息
    private String instanceId;
    private String hostname;
    private Long timestamp;

    //扩展信息
    private Map<String, Object> extraInfo;

}
