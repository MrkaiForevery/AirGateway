package com.airfree.mq.cofig.kafka;

import lombok.Data;


@Data
public class KafkaConsumerConfig {

    private String bootstrapServers;
    private String groupId;
    private String keyDeserializer;
    private String valueDeserializer;
    private String topics;
    private String listenerClass; // 监听器类全限定名
    private String instanceNums; // 创建多少个同类型Consumer
}
