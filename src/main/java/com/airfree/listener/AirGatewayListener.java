package com.airfree.listener;

import org.springframework.context.ApplicationEvent;

public interface AirGatewayListener<T extends ApplicationEvent> {

    void onCacheRefresh(T event);

    void onCacheEvict(T event);

    void onCacheLoad(T event);

    /**
     * 支持的缓存名称模式，支持通配符
     */
    default boolean supports(String cacheName) {
        return true;
    }

    /**
     * 监听器执行顺序
     */
    default int getOrder() {
        return 0;
    }
}
