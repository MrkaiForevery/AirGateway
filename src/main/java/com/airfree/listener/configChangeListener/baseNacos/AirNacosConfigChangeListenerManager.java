package com.airfree.listener.configChangeListener.baseNacos;

import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.listener.impl.AbstractConfigChangeListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirNacosConfigChangeListenerManager {

    private final ApplicationContext applicationContext;

    private final NacosConfigManager nacosConfigManager;

    private final Map<Class<? extends AbstractConfigChangeListener>, AbstractConfigChangeListener> listenerMap = new ConcurrentHashMap<>();

    public AirNacosConfigChangeListenerManager(ApplicationContext applicationContext, NacosConfigManager configManager) {
        this.applicationContext = applicationContext;
        this.nacosConfigManager = configManager;
        initListenerMap();
        log.info("构建所有的nacosConfig-listener完成！信息为{}", this.listenerMap);
    }

    private void initListenerMap() {
        List<NacosConfigChangeElementType> elementTypes = Arrays.asList(NacosConfigChangeElementType.values());
        elementTypes.forEach(e -> {
            AbstractConfigChangeListener listener = applicationContext.getBeansOfType(e.getListenerClass()).values().stream().collect(Collectors.toList()).get(0);
            try {
                //todo 这里必须要先把listener注册到nacosConfigManager里面，不然监听不到配置变化。
                this.nacosConfigManager.getConfigService().addListener(e.getDataId(), e.getGroup(), listener);
                listenerMap.put(e.getListenerClass(), listener);
                log.info("构建nacosConfig-listener成功！信息为:{}", listener);
            } catch (NacosException ex) {
                log.error("构建nacosConfig-listener失败！信息为:{}", listener);
                ex.printStackTrace();
            }
        });
    }

}
