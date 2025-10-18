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
public class AirRocketMqBatchProducerFactory {

    private final AirRocketMQConfigProperties rocketMQConfigProperties;
    private Map<String, RocketMQTemplate> producerRocketMQTemplateMap = new ConcurrentHashMap<>();

    public AirRocketMqBatchProducerFactory(AirRocketMQConfigProperties rocketMQConfigProperties) {
        this.rocketMQConfigProperties = rocketMQConfigProperties;
        log.info("开始批量创建RocketMqProducer......");
        //创建所有的producer
        Map<String, ProducerConfig> producerConfigMap = rocketMQConfigProperties.getProducers();
        buildProducerRocketMQTemplate(producerConfigMap);
        log.info("批量创建RocketMqProducer成功，producerRocketMQTemplateMap信息为：{}", this.producerRocketMQTemplateMap);
    }

    private void buildProducerRocketMQTemplate(Map<String, ProducerConfig> producerConfigMap) {
        // Producer只负责发送，不关心谁接收、如何过滤，所以不需要进行tag定义，但是可以同时对多个topic进行广播
        producerConfigMap.entrySet().forEach(entry -> {
            ProducerConfig producerConfig = entry.getValue();
            Assert.notNull(producerConfig, entry.getKey() + "producer configuration cannot be null");
            RocketMQTemplate template = new RocketMQTemplate();
            List<String> topics = producerConfig.getTopics().values().stream().collect(Collectors.toList());
            Assert.isTrue(topics.size() != 0, entry.getKey() + "producer topics configuration cannot be null");
            DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerConfig.getGroup());
            defaultMQProducer.setTopics(topics);
            template.setProducer(defaultMQProducer);
            this.producerRocketMQTemplateMap.put(entry.getKey(), template);
            log.info("开始创建RocketMqProducer成功，group:{} producerName:{}  topics信息:{}", producerConfig.getGroup(), entry.getKey(), topics);
        });
    }
}
