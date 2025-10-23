package com.airfree.mq.core;

import com.airfree.mq.cofig.kafka.AirKafkaConfigProperties;
import com.airfree.mq.cofig.kafka.KafkaProducerConfig;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirKafkaBatchSenderFactory {

    private final AirKafkaConfigProperties kafkaConfigProperties;
    private final Map<String, KafkaSender<?, ?>> kafkaSenderMap = new ConcurrentHashMap<>();

    public AirKafkaBatchSenderFactory(AirKafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        Map<String, KafkaProducerConfig> producersConfig = kafkaConfigProperties.getProducers();
        //批量创建producer
        producersConfig.entrySet().forEach(entry -> {
            log.info("开始创建单个KafkaSender....");
            KafkaSender<?, ?> sender = createOneSender(entry.getKey(), entry.getValue());
            log.info("创建KafkaSender: {}成功！！！", sender);
        });
        log.info("批量创建KafkaSender成功！！！当前KafkaSender数量{}", kafkaSenderMap.size());
    }

    public KafkaSender<?, ?> createOneSender(String senderName, KafkaProducerConfig senderConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, senderConfig.getBootstrapServers());
        //通过反射获取
        try {
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(senderConfig.getKeySerializer()));
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(senderConfig.getValueSerializer()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        //todo 更多参数后面配置吧，这里先搞这么多
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, senderConfig.getBatchSize());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, senderConfig.getAck());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG, senderConfig.getBufferMemory());
        SenderOptions<?, ?> senderOptions = SenderOptions.create(props);
        //创建
        KafkaSender<?, ?> sender = KafkaSender.create(senderOptions);
        kafkaSenderMap.put(senderName, sender);
        return sender;
    }

    @PreDestroy
    public void destroy() {
        log.info("正在销毁所有 Kafka-Producer 生产者...");
        kafkaSenderMap.values().forEach(sender -> {
            sender.close();
        });
        kafkaSenderMap.clear();
    }
}
