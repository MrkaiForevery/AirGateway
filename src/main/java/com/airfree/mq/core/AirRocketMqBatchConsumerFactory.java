package com.airfree.mq.core;

import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import com.airfree.mq.cofig.rocketMq.ConsumerConfig;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirRocketMqBatchConsumerFactory {

    private final AirRocketMQConfigProperties rocketMQConfigProperties;

    private final ApplicationContext applicationContext;

    private DefaultRocketMQListenerContainer containerFactory;

    private final Map<String, DefaultRocketMQListenerContainer> consumerContainers = new ConcurrentHashMap<>();

    public AirRocketMqBatchConsumerFactory(AirRocketMQConfigProperties rocketMQConfigProperties,ApplicationContext applicationContext) {
        this.applicationContext= applicationContext;
        this.rocketMQConfigProperties = rocketMQConfigProperties;
        log.info("开始批量创建 RocketMQ 消费者...");
        this.containerFactory = new DefaultRocketMQListenerContainer();
        List<ConsumerConfig> consumerConfigs = rocketMQConfigProperties.getConsumers().values().stream().collect(Collectors.toList());

        for (ConsumerConfig config : consumerConfigs) {
            try {
                createAndStartConsumer(config);
                log.info("成功创建消费者: group={}, topic={}, tag={}",
                        config.getGroup(), config.getTopic(), config.getTag());
            } catch (Exception e) {
                log.error("创建消费者失败: group={}, topic={}", config.getGroup(), config.getTopic(), e);
            }
        }
        log.info("批量创建 RocketMQ 消费者完成，总计: {}", consumerContainers.size());
    }

    public void createAndStartConsumer(ConsumerConfig config) throws ClassNotFoundException {
        //todo 使用反射创建消息监听器
        MessageListenerConcurrently messageListenerConcurrently = null;
        MessageListenerOrderly listenerOrderly  = null;
        if (config.getListenerClass() != null) {
            // 通过类名创建监听器实例
            Class<?> listenerClass = Class.forName(config.getListenerClass());
            Object listenerInstance = applicationContext.getAutowireCapableBeanFactory().createBean(listenerClass);
            if (listenerInstance instanceof MessageListenerConcurrently ) {
                messageListenerConcurrently = (MessageListenerConcurrently) listenerInstance;
            }else if (listenerInstance instanceof MessageListenerOrderly) {
                 MessageListenerOrderly listenerInstance1 = (MessageListenerOrderly) listenerInstance;
            }else {
                throw new RuntimeException("无法创建的rocketMQListener类型");
            }
        }else{
            throw new RuntimeException("无法创建rocketMQListener:topic不能为null");
        }

        // 配置监听器容器
        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();
        container.setNameServer(rocketMQConfigProperties.getNameServer());
        container.setConsumerGroup(config.getGroup());
        container.setTopic(config.getTopic());
        container.setSelectorExpression(config.getTag());

        // 设置消费者参数 todo 这里可以实验一下还可以设置哪些参数
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getGroup());
        consumer.setNamesrvAddr(rocketMQConfigProperties.getNameServer());
        // 注册消费者的回调接口处理消息
        if (messageListenerConcurrently != null) {
            consumer.registerMessageListener(messageListenerConcurrently);
        }else if (listenerOrderly != null) {
            consumer.registerMessageListener(messageListenerConcurrently);
        }

        // 启动消费者
        container.setConsumer(consumer);
        container.start();

        // 保存容器引用
        consumerContainers.put(config.getGroup(), container);
    }


    public void shutdownConsumer(String consumerGroup) {
        DefaultRocketMQListenerContainer container = consumerContainers.get(consumerGroup);
        if (container != null) {
            container.getConsumer().shutdown();
            consumerContainers.remove(consumerGroup);
            log.info("消费者已关闭: {}", consumerGroup);
        }
    }

    @PreDestroy
    public void destroy() {
        log.info("正在关闭所有 RocketMQ 消费者...");
        consumerContainers.values().forEach(container -> {
            container.getConsumer().shutdown();
        });
        consumerContainers.clear();
    }
}
