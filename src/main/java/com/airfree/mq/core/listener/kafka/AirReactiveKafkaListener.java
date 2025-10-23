package com.airfree.mq.core.listener.kafka;

import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public interface AirReactiveKafkaListener<K, V> {
    /**
     * 处理接收到的消息
     */
    Mono<Void> onMessage(ReceiverRecord<K, V> record);

    /**
     * 返回该监听器支持的主题（支持通配符）
     */
    String getSupportedTopic();

    /**
     * 返回监听器名称（用于标识）
     */
    String getListenerName();

    /**
     * 是否启用该监听器
     */
    default boolean isEnabled() {
        return true;
    }
}
