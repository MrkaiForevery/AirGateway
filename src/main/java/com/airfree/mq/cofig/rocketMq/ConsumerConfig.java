package com.airfree.mq.cofig.rocketMq;

import lombok.Data;

@Data
public class ConsumerConfig {

    private String group;
    private String topic;
    private String tag;
    private String listenerClass; // 监听器类全限定名
}
