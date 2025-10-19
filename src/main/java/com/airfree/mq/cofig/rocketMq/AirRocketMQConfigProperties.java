package com.airfree.mq.cofig.rocketMq;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@RefreshScope
@ConfigurationProperties(prefix = "rocketmq")
public class AirRocketMQConfigProperties {

    private String nameServer;
    private Map<String, ProducerConfig> producers;
    private Map<String, ConsumerConfig> consumers;

}
