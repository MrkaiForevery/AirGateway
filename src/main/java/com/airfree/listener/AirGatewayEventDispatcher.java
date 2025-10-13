package com.airfree.listener;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.cache.AirGatewayCacheEventListener;
import com.airfree.log.AirGatewayAbstractLogEvent;
import com.airfree.log.AirGatewayLogListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 自定义事件分发器*
 */
@Slf4j
@Component
public class AirGatewayEventDispatcher implements ApplicationListener<ApplicationEvent> {

    //缓存监听器集合,只监听cache刷新类事件 todo 如果有其他类型的listener也可以注入进来
    private final List<AirGatewayCacheEventListener<?>> cacheEventListenerList = new CopyOnWriteArrayList<>();
    //缓存监听器集合,只监听log刷新类事件
    private final List<AirGatewayLogListener<?>> logEventListenerList = new CopyOnWriteArrayList<>();


    @Autowired(required = false)
    public void setCacheListeners(List<AirGatewayCacheEventListener<?>> listeners) {
        this.cacheEventListenerList.clear();
        this.cacheEventListenerList.addAll(listeners);
        this.cacheEventListenerList.sort(Comparator.comparing(AirGatewayCacheEventListener::getOrder));
    }

    @Autowired(required = false)
    public void setLogListeners(List<AirGatewayLogListener<?>> listeners) {
        this.logEventListenerList.clear();
        this.logEventListenerList.addAll(listeners);
        this.logEventListenerList.sort(Comparator.comparing(AirGatewayLogListener::getOrder));
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof AirGatewayAbstractCacheEvent) {
            for (AirGatewayCacheEventListener listener : cacheEventListenerList) {
                try {
                    // 通过反射调用具体的onEvent方法
                    invokeCacheTypeListener(listener, event);
                } catch (Exception e) {
                    log.error("事件处理失败: {}", listener.getClass().getSimpleName(), e);
                }
            }
        }

        if (event instanceof AirGatewayAbstractLogEvent) {
            for (AirGatewayLogListener listener : logEventListenerList) {
                try {
                    // 通过反射调用具体的onEvent方法
                    invokeLogEventListener(listener, event);
                } catch (Exception e) {
                    log.error("事件处理失败: {}", listener.getClass().getSimpleName(), e);
                }
            }
        }
    }

    private <T extends AirGatewayAbstractCacheEvent> void invokeCacheTypeListener(AirGatewayCacheEventListener<T> listener, ApplicationEvent event) {
        listener.onCacheRefresh((T) event);
    }

    private <T extends AirGatewayAbstractLogEvent> void invokeLogEventListener(AirGatewayLogListener<T> listener, ApplicationEvent event) {

        listener.onLogRecordFish((T) event);
    }

    @Override
    public boolean supportsAsyncExecution() {
        return ApplicationListener.super.supportsAsyncExecution();
    }


}
