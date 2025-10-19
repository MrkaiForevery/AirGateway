package com.airfree.listener.configChangeListener.baseNacos;

import com.airfree.listener.configChangeListener.baseNacos.nacosConfigChangeListener.RocketMqConfigChangeListener;
import com.alibaba.nacos.client.config.listener.impl.AbstractConfigChangeListener;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum NacosConfigChangeElementType {

    airRocketMQConfigProperties("airRocketMQConfigProperties.yaml","AIR_GROUP", RocketMqConfigChangeListener.class);

    private String dataId;
    private String group;
    private Class<? extends AbstractConfigChangeListener>  listenerClass;

}
