package com.airfree.log.core.storage;

import com.airfree.entity.log.AirGatewayLog;
import reactor.core.publisher.Mono;

import java.util.List;

public interface AirGatewayLogStorage {

    void saveAccessLog(AirGatewayLog gatewayLog);

    void saveErrorLog(AirGatewayLog gatewayLog);

    void saveBusinessLog(AirGatewayLog gatewayLog);

    Mono<LogPageResult<AirGatewayLog>> queryAccessLogs(LogQueryCondition condition);

    Mono<List<AirGatewayLog>> queryErrorLogs(LogQueryCondition condition);
}
