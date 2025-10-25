package com.airfree.discovery.enums;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.discoveryService.AirConsulReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirEurekaReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirNacosReactiveDiscovery;
import com.airfree.discovery.discoveryService.AirZookeeperReactiveDiscovery;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AirServiceDiscoveryTypeEnum {

    NACOS_DISCOVERY("nacos", AirNacosReactiveDiscovery.class),
    ZOOKEEPER_DISCOVERY("zookeeper", AirZookeeperReactiveDiscovery.class),
    CONSUL_DISCOVERY("consul", AirConsulReactiveDiscovery.class),
    EUREKA_DISCOVERY("eureka", AirEurekaReactiveDiscovery.class);

    private String typeName;
    private Class<? extends AirReactiveServiceDiscovery> reflectClass;

    /**
     * 根据类型名称查找枚举
     */
    public static AirServiceDiscoveryTypeEnum findDiscoveryTypeEnum(String type) {
        for (AirServiceDiscoveryTypeEnum typeEnum : AirServiceDiscoveryTypeEnum.values()) {
            if (typeEnum.getTypeName().equalsIgnoreCase(type)) {
                return typeEnum;
            }
        }
        return null;
    }

}
