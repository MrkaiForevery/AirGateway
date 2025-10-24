package com.airfree.discovery.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AirServiceDiscoveryTypeEnum {

    NACOS_DISCOVERY("nacos"),
    ZOOKEEPER_DISCOVERY("zookeeper"),
    CONSUL_DISCOVERY("consul"),
    EUREKA_DISCOVERY("eureka");

    private String typeName;
}
