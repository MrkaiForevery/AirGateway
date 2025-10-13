package com.airfree.mq.cofig.rocketMq;

import com.airfree.mq.cofig.AbstractAirMqConfig;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;


@Getter
@Setter
@Component
public class AirRocketMqConfig extends AbstractAirMqConfig {

    private String nameSrvAddress;
    private List<ProducerConfig> producerConfigList;
    private List<ConsumerConfig> consumerConfigList;

    public AirRocketMqConfig(){
        flushConfig();

    }

    public static void flushConfig() {

    }

    @Override
    public String getMqTypeName() {
        return this.getClass().getSimpleName();
    }

    public static class ProducerConfig {
        private String producerGroup;
        private String topicName;
    }

    public static class ConsumerConfig {
        private String consumerGroup;
        private String topicName;
    }
}
