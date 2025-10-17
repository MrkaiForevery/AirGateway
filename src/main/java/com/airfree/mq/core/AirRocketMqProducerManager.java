package com.airfree.mq.core;

import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import com.airfree.mq.cofig.rocketMq.ProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirRocketMqProducerManager {

    private final AirRocketMQConfigProperties rocketMQConfigProperties;

    private Map<String,RocketMQTemplate> producerRocketMQTemplateMap = new ConcurrentHashMap<>();

    public AirRocketMqProducerManager(AirRocketMQConfigProperties rocketMQConfigProperties) {
        this.rocketMQConfigProperties = rocketMQConfigProperties;
        log.info("打印从nacos上读取的AirRocketMQConfigProperties信息:{}",rocketMQConfigProperties);
        log.info("开始批量创建RocketMqProducer......");
        //创建所有的producer
        Map<String, ProducerConfig> producerConfigMap = rocketMQConfigProperties.getProducers();
        buildProducerRocketMQTemplate(producerConfigMap);
    }

    private void buildProducerRocketMQTemplate(Map<String, ProducerConfig> producerConfigMap ) {
        producerConfigMap.entrySet().forEach(entry ->{
            ProducerConfig producerConfig = entry.getValue();
            Assert.notNull(producerConfig, "producer1 configuration cannot be null");
            RocketMQTemplate template = new RocketMQTemplate();
            DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerConfig.getGroup());
            List<String> topics = producerConfig.getTopics().values().stream().collect(Collectors.toList());
            defaultMQProducer.setTopics(topics);
            template.setProducer(defaultMQProducer);
            this.producerRocketMQTemplateMap.put(entry.getKey(),template);
        });
    }
}
