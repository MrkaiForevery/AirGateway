package com.airfree.mq.cofig.rocketMq;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ProducerConfig {
    private String group;
    private Map<String,String> topics = new HashMap<>();
}
