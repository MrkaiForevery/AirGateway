package com.airfree.mq.core;

import com.airfree.mq.cofig.kafka.AirKafkaConfigProperties;
import com.airfree.mq.cofig.kafka.KafkaConsumerConfig;
import com.airfree.mq.core.listener.kafka.AirReactiveKafkaListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AirKafkaBatchReceiverFactory {

    private final AirKafkaConfigProperties kafkaConfigProperties;
    private final ApplicationContext applicationContext;
    private final Map<String, KafkaReceiver<?, ?>> kafkaReceiverMap = new ConcurrentHashMap<>();

    public AirKafkaBatchReceiverFactory(ApplicationContext applicationContext, AirKafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        this.applicationContext = applicationContext;
        Map<String, KafkaConsumerConfig> consumersConfig = kafkaConfigProperties.getConsumers();
        //批量创建KafkaReceiver
        consumersConfig.entrySet().forEach(entry -> {
            log.info("开始创建单个KafkaReceiver....");
            KafkaReceiver<?, ?> receiver = null;
            try {
                //支持创建多个同样的Receiver
                createOneTypeReceiverInstances(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("创建KafkaReceiver:{}成功", receiver);
        });
        log.info("批量创建KafkaReceiver成功！！！当前KafkaReceiver数量{}", kafkaReceiverMap.size());
    }

    public void createOneTypeReceiverInstances(String receiverName, KafkaConsumerConfig receiverConfig) {
        int instanceNums = 1;
        if (receiverConfig.getInstanceNums() != null) {
            instanceNums = Integer.parseInt(receiverConfig.getInstanceNums());
        }

        for (int i = 0; i < instanceNums; i++) {
            String instanceName = receiverName + "[" + i + "]-" + System.currentTimeMillis();
            try {
                createOneReceiverInstance(instanceName, receiverConfig);
            } catch (Exception e) {
                log.info("创建{}实例失败！！！", instanceName);
                e.printStackTrace();
            }
        }
    }

    public <K, V> void createOneReceiverInstance(String receiverName, KafkaConsumerConfig receiverConfig) throws Exception {
        // 1. 先创建监听器实例，获取其泛型类型信息
        AirReactiveKafkaListener<K, V> listener = createListenerInstance(receiverConfig);
        // 2. 创建 KafkaReceiver，使用与监听器相同的泛型类型
        KafkaReceiver<K, V> kafkaReceiver = createKafkaReceiver(receiverConfig);
        //给这个receiver绑定这个listener
        if (listener.isEnabled()) {
            bindListenerToReceiver(kafkaReceiver, listener);
        } else {
            throw new Exception("rocketMQListener为禁用状态，无法被绑定！！！");
        }
        kafkaReceiverMap.put(receiverName, kafkaReceiver);
    }

    private <K, V> AirReactiveKafkaListener<K, V> createListenerInstance(KafkaConsumerConfig receiverConfig) throws Exception {
        String listenerClassName = receiverConfig.getListenerClass();

        try {
            Class<?> listenerClass = Class.forName(listenerClassName);
            Object listenerInstance = applicationContext.getAutowireCapableBeanFactory().createBean(listenerClass);

            if (listenerInstance instanceof AirReactiveKafkaListener) {
                return (AirReactiveKafkaListener<K, V>) listenerInstance;
            } else {
                throw new Exception("类 " + listenerClassName + " 未实现 AirReactiveKafkaListener 接口");
            }
        } catch (ClassNotFoundException e) {
            throw new Exception("监听器类未找到: " + listenerClassName, e);
        } catch (Exception e) {
            throw new Exception("反射创建监听器发生异常: " + listenerClassName, e);
        }
    }

    private <K, V> KafkaReceiver<K, V> createKafkaReceiver(KafkaConsumerConfig receiverConfig) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, receiverConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, receiverConfig.getGroupId());

        // 通过反射实例化 Deserializer
        try {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(receiverConfig.getKeyDeserializer()));
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(receiverConfig.getValueDeserializer()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("反序列化器类未找到", e);
        }

        // 订阅topic
        List<String> topics = Arrays.asList(receiverConfig.getTopics().split(","));
        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.<K, V>create(props).subscription(topics);

        // 创建KafkaReceiver
        return KafkaReceiver.create(receiverOptions);
    }

    public <K, V> void bindListenerToReceiver(KafkaReceiver<K, V> kafkaReceiver, AirReactiveKafkaListener<K, V> listenerInstance) {

        String topic = listenerInstance.getSupportedTopic();
        String listenerName = listenerInstance.getListenerName();
        log.info("正在绑定监听器: {} 到主题: {}", listenerName, topic);

        kafkaReceiver.receive()
                .concatMap(record -> {
                    log.debug("开始处理消息 - Topic: {}, Key: {}", record.topic(), record.key());
                    return listenerInstance.onMessage(record)
                            .doOnSuccess(v -> {
                                log.debug("消息处理完成 - Topic: {}, Key: {}", record.topic(), record.key());
                                // 手动提交偏移量
                                record.receiverOffset().acknowledge();
                            })
                            .doOnError(error -> log.error("消息处理失败 - Topic: {}, Key: {}", record.topic(), record.key(), error));
                })
                .onErrorContinue((error, obj) -> {
                    log.error("消息处理流程发生错误: {}", error.getMessage(), error);
                })
                .subscribe();

        log.info("成功绑定监听器: {} 到主题: {}", listenerName, topic);
    }
}
