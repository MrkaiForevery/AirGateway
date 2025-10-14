package com.airfree.cache.cacheEnums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum AirGatewayCacheEventOperationEnum {

    DEPEND_BASE_LOCAL_ADD("local","add"),
    DEPEND_BASE_LOCAL_UPDATE("local","update"),
    DEPEND_BASE_LOCAL_DELETED("local","deleted"),

    DEPEND_BASE_REDIS_ADD("redis","add"),
    DEPEND_BASE_REDIS_UPDATE("redis","update"),
    DEPEND_BASE_REDIS_DELETED("redis","deleted");

    private String dependBase;

    private String operationType;
}
