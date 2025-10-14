package com.airfree.listener;

import com.airfree.cache.AirGatewayAbstractCacheEvent;
import com.airfree.cache.AirGatewayCacheEventListener;
import com.airfree.cache.cacheEnums.AirGatewayCacheEventOperationEnum;
import com.airfree.log.AirGatewayAbstractLogEvent;
import com.airfree.log.AirGatewayLogListener;
import lombok.extern.slf4j.Slf4j;
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

    private final static String localCacheDependBase = AirGatewayCacheEventOperationEnum.DEPEND_BASE_LOCAL_ADD.getDependBase();
    private final static String redisCacheDependBase = AirGatewayCacheEventOperationEnum.DEPEND_BASE_REDIS_ADD.getDependBase();


    public AirGatewayEventDispatcher(List<AirGatewayCacheEventListener<?>> cacheEventListenerList,
                                     List<AirGatewayLogListener<?>> logEventListenerList) {
        setCacheListeners(cacheEventListenerList);
        setLogListeners(logEventListenerList);
        log.info("AirGatewayEventDispatcher事件分发器初始化成功！！！");
    }

    private void setCacheListeners(List<AirGatewayCacheEventListener<?>> listeners) {
        this.cacheEventListenerList.clear();
        this.cacheEventListenerList.addAll(listeners);
        this.cacheEventListenerList.sort(Comparator.comparing(AirGatewayCacheEventListener::getOrder));
        log.info("注册全部的cacheEventListenerList: " + this.cacheEventListenerList);
    }


    private void setLogListeners(List<AirGatewayLogListener<?>> listeners) {
        this.logEventListenerList.clear();
        this.logEventListenerList.addAll(listeners);
        this.logEventListenerList.sort(Comparator.comparing(AirGatewayLogListener::getOrder));
        log.info("注册全部的logEventListenerList: " + this.logEventListenerList);
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

    private <T extends AirGatewayAbstractCacheEvent> void invokeCacheTypeListener(AirGatewayCacheEventListener<T> listener, ApplicationEvent event) throws Exception {
        T airGatewayAbstractCacheEvent = (T) event;
        AirGatewayCacheEventOperationEnum operation = airGatewayAbstractCacheEvent.getOperation();
        if (operation == null || operation.getDependBase() == null) {
            throw new Exception("不支持cache刷新操作类型，停止刷新！！！");
        }
        String dependBase = operation.getDependBase();
        if (localCacheDependBase.equals(dependBase)) {
            listener.onLocalReFlush(airGatewayAbstractCacheEvent);
        }

        if (redisCacheDependBase.equals(dependBase)) {
            listener.onRedisReFlush(airGatewayAbstractCacheEvent);
        }
    }

    private <T extends AirGatewayAbstractLogEvent> void invokeLogEventListener(AirGatewayLogListener<T> listener, ApplicationEvent event) {
        //TODO 待完成
        listener.onLogRecordFish((T) event);
    }

    @Override
    public boolean supportsAsyncExecution() {
        return ApplicationListener.super.supportsAsyncExecution();
    }
}
