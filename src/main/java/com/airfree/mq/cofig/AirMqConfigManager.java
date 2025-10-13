package com.airfree.mq.cofig;

import com.airfree.cache.AirGatewayCacheEventListener;
import com.airfree.cache.event.cofngReflushEvent.MqConfigFlushEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirMqConfigManager implements AirGatewayCacheEventListener<MqConfigFlushEvent> {

    @Resource
    private ApplicationContext applicationContext;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    private final Map<AbstractAirMqConfigLoader, AbstractAirMqConfig> airMqConfigMap = new ConcurrentHashMap<>();
    private final List<AbstractAirMqConfigLoader> airMqConfigLoaderList = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        //获取所有的MqConfigLoader
        airMqConfigLoaderList.addAll(
                applicationContext
                        .getBeansOfType(AbstractAirMqConfigLoader.class)
                        .entrySet().stream()
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList()));
        //获取所有的MqConfig,并把所有的config绑定对应的configLoader
        applicationContext.getBeansOfType(AbstractAirMqConfig.class).entrySet().stream().forEach(entry -> {
            Class<? extends AbstractAirMqConfig> configClass = entry.getValue().getClass();
            try {
                buildMqConfig(configClass, entry.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void buildMqConfig(Class<? extends AbstractAirMqConfig> configClass, AbstractAirMqConfig config) throws Exception {
        List<? extends Class<? extends AbstractAirMqConfig>> supportConfigClassList = airMqConfigLoaderList.stream()
                .map(AbstractAirMqConfigLoader::supportMqConfigType)
                .collect(Collectors.toList());

        if (supportConfigClassList.contains(configClass)) {
            airMqConfigLoaderList.stream().forEach(e -> {
                if (e.supportMqConfigType().getClass().isInstance(configClass)) {
                    airMqConfigMap.put(e, config);
                }
            });
        } else {
            throw new Exception("不支持mqConfig类型:" + configClass.getClass().toGenericString());
        }
    }

    @Override
    public void onCacheRefresh(MqConfigFlushEvent event) {
        log.info("监听到mq配置缓存刷新事件，正在处理中......");
    }

    @Override
    public void onCacheEvict(MqConfigFlushEvent event) {

    }

    @Override
    public void onCacheLoad(MqConfigFlushEvent event) {
        log.info("监听到mq配置缓存加载事件，正在处理中......");
    }

    @Override
    public boolean supports(String cacheName) {
        return AirGatewayCacheEventListener.super.supports(cacheName);
    }

    @Override
    public int getOrder() {
        return AirGatewayCacheEventListener.super.getOrder();
    }
}
