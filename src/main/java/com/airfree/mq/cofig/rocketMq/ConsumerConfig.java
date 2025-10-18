package com.airfree.mq.cofig.rocketMq;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ConsumerConfig {

    private String group;
    private String topic;
    private Map<String,String> tags =new HashMap<>();
    private String listenerClass; // 监听器类全限定名
    private String instanceNums; //启动多少个监听器去处理
}
