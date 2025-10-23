package com.airfree.mq.cofig.kafka;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@ConfigurationProperties(prefix = "kafka")
public class AirKafkaConfigProperties {

    private Map<String, KafkaProducerConfig> producers;
    private Map<String, KafkaConsumerConfig> consumers;

}
