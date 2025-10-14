package com.airfree.log.core.storage;

import com.airfree.entity.log.AirGatewayLog;
import com.airfree.log.config.AirGatewayLogProperties;
import com.airfree.log.logEnums.AirGatewayLogTypeEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Slf4j
@Component
public class DefaultAirGatewayLogStorage implements AirGatewayLogStorage {

    private final AirGatewayLogProperties properties;

    // 可以根据需要注入不同的存储客户端
    // private final MongoTemplate mongoTemplate;
    // private final ElasticsearchRestTemplate elasticsearchTemplate;
    // private final RedisTemplate<String, Object> redisTemplate;

    public DefaultAirGatewayLogStorage(AirGatewayLogProperties properties) {
        this.properties = properties;
        log.info("DefaultAirGatewayLogStorage初始化完成！");
    }

    @Override
    public void saveAccessLog(AirGatewayLog gatewayLog) {
        // 根据配置选择存储方式
        switch (properties.getStorageType()) {
            case "ELASTICSEARCH":
                saveToElasticsearch(gatewayLog, "gateway-access-log");
                break;
            case "MONGO":
                saveToMongo(gatewayLog, "access_log");
                break;
            case "FILE":
                saveToFile(gatewayLog, "access");
                break;
            case "CONSOLE":
            default:
                logToConsole(gatewayLog);
                break;
        }
    }

    @Override
    public void saveErrorLog(AirGatewayLog gatewayLog) {
        switch (properties.getStorageType()) {
            case "ELASTICSEARCH":
                saveToElasticsearch(gatewayLog, "gateway-error-log");
                break;
            case "MONGO":
                saveToMongo(gatewayLog, "error_log");
                break;
            case "FILE":
                saveToFile(gatewayLog, "error");
                break;
            case "CONSOLE":
            default:
                logToConsole(gatewayLog);
                break;
        }
    }

    @Override
    public void saveBusinessLog(AirGatewayLog gatewayLog) {
        // 实现类似...
    }

    private void saveToElasticsearch(AirGatewayLog gatewayLog, String index) {
        // 使用 Elasticsearch 存储
        // elasticsearchTemplate.save(gatewayLog, index);
    }

    private void saveToMongo(AirGatewayLog gatewayLog, String collection) {
        // 使用 MongoDB 存储
        // mongoTemplate.save(gatewayLog, collection);
    }

    private void saveToFile(AirGatewayLog gatewayLog, String type) {
        try {
            String logFile = properties.getLogFilePath() + "/gateway-" + type + ".log";
            String logContent = convertToLogString(gatewayLog);

            Files.write(Paths.get(logFile),
                    (logContent + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("写入日志文件失败", e);
        }
    }

    private void logToConsole(AirGatewayLog gatewayLog) {
        if (AirGatewayLogTypeEnum.ACCESS.name().equals(gatewayLog.getExtraInfo().get("logType"))) {
            log.info("Gateway Access Log: {}", gatewayLog);
        } else if (AirGatewayLogTypeEnum.ERROR.name().equals(gatewayLog.getExtraInfo().get("logType"))) {
            log.error("Gateway Error Log: {}", gatewayLog);
        }
    }

    private String convertToLogString(AirGatewayLog gatewayLog) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(gatewayLog);
        } catch (JsonProcessingException e) {
            return gatewayLog.toString();
        }
    }

    @Override
    public Mono<LogPageResult<AirGatewayLog>> queryAccessLogs(LogQueryCondition condition) {
        // 实现查询逻辑
        return Mono.empty();
    }

    @Override
    public Mono<List<AirGatewayLog>> queryErrorLogs(LogQueryCondition condition) {
        // 实现查询逻辑
        return Mono.empty();
    }
}