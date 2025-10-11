package com.airfree.log.core.storage;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.data.web.SpringDataWebProperties;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogQueryCondition {

    // 时间范围
    private Long startTime;
    private Long endTime;

    // 请求信息
    private String traceId;
    private String requestId;
    private String method;
    private String path;
    private Integer statusCode;

    // 业务信息
    private String serviceId;
    private String routeId;

    // 错误信息
    private String errorMessage;
    private String errorType;

    // 分页信息
    private Integer page = 1;
    private Integer size = 20;
    private String sortField = "timestamp";
//    private SpringDataWebProperties.Sort.Direction sortDirection = Sort.Direction.DESC;

    // 扩展查询条件
    private Map<String, Object> extraConditions;
}
