package com.airfree.cache.caheEventPublisner;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConfigFlushEventPublisher {

    private final ApplicationEventPublisher eventPublisher;

    private final Object mqConfigFlushData = new Object();

    public ConfigFlushEventPublisher(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void syncPublishEvent(AirGatewayAbstractCacheEvent event){
        //发布刷新事件
        eventPublisher.publishEvent(event);
        log.info("同步发布缓存刷新事件: cache={}, type={}, key={}, reason={}");
    }

    //todo 这个注解要搞清楚怎么回事
    @Async("cacheEventTaskExecutor")
    public void asyncPublishEvent(AirGatewayAbstractCacheEvent event){
        //发布刷新事件
        syncPublishEvent(event);
        log.info("异步发送发布缓存刷新事件: cache={}, type={}, key={}, reason={}");
    }
}