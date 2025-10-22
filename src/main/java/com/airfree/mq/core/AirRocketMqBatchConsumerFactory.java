package com.airfree.mq.core;

import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import com.airfree.mq.cofig.rocketMq.ConsumerConfig;
import com.airfree.mq.index.ConsumerIndex;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirRocketMqBatchConsumerFactory {

    private final AirRocketMQConfigProperties rocketMQConfigProperties;
    private final ApplicationContext applicationContext;
    private final Map<ConsumerIndex, MQPushConsumer> consumerContainers = new ConcurrentHashMap<>();

    public AirRocketMqBatchConsumerFactory(AirRocketMQConfigProperties rocketMQConfigProperties, ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        this.rocketMQConfigProperties = rocketMQConfigProperties;

        log.info("开始批量创建 RocketMQ 消费者...");
        List<ConsumerConfig> consumerConfigs = rocketMQConfigProperties.getConsumers().values().stream().collect(Collectors.toList());
        for (ConsumerConfig config : consumerConfigs) {
            try {
                buildOneTypeConsumer(config);
                log.info("成功创建消费者: group={}, topic={}, tag={}",
                        config.getGroup(), config.getTopic(), config.getTags());
            } catch (Exception e) {
                log.error("创建消费者失败: group={}, topic={}", config.getGroup(), config.getTopic(), e);
            }
        }
        log.info("批量创建 RocketMQ 消费者完成，总计: {},消费者实例信息为{}", consumerContainers.size(),consumerContainers.keySet());

    }

    public synchronized void buildOneTypeConsumer(ConsumerConfig config) throws ClassNotFoundException, MQClientException {
        Integer instanceNums = 1;
        if (config.getInstanceNums() != null) {
            instanceNums = Integer.valueOf(config.getInstanceNums());
        }
        for (int i = 0; i < instanceNums; i++) {
            //这里只有监听新增加操作，因此不需要判断原有的consumer状态，然后stop it以后移除的操作
            createSingleConsumerInstanceAndStartIt(config, i);
        }
        log.info("构建同一消费者实例成功！。。。。");
    }

    private void createSingleConsumerInstanceAndStartIt(ConsumerConfig config, int index) throws ClassNotFoundException, MQClientException {
        MessageListenerConcurrently messageListenerConcurrently = null;
        MessageListenerOrderly listenerOrderly = null;
        if (config.getListenerClass() != null) {
            // 通过类名创建监听器实例
            Class<?> listenerClass = Class.forName(config.getListenerClass());
            Object listenerInstance = applicationContext.getAutowireCapableBeanFactory().createBean(listenerClass);
            if (listenerInstance instanceof MessageListenerConcurrently) {
                messageListenerConcurrently = (MessageListenerConcurrently) listenerInstance;
            } else if (listenerInstance instanceof MessageListenerOrderly) {
                listenerOrderly = (MessageListenerOrderly) listenerInstance;
            } else {
                throw new RuntimeException("无法创建的rocketMQListener类型,仅支持MessageListenerConcurrently和MessageListenerOrderly两种类型");
            }
        } else {
            throw new RuntimeException("无法创建rocketMQListener:topic不能为null");
        }
        // 创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getGroup());
        consumer.setNamesrvAddr(rocketMQConfigProperties.getNameServer());
        // 设置实例名称，用于区分多个实例
        consumer.setInstanceName(config.getGroup() + "-instance-" + index + System.currentTimeMillis());
        // 配置消费者参数，避免频繁路由更新
        configureConsumerForStability(consumer);
        // 订阅Topic和Tag
        String tags = StringUtils.collectionToDelimitedString(new ArrayList<>(config.getTags().values()), "||");
        try {
            consumer.subscribe(config.getTopic(), tags);
        } catch (MQClientException e) {
            throw new RuntimeException("订阅Topic失败", e);
        }

        // 注册消费者的回调接口处理消息
        if (messageListenerConcurrently != null) {
            consumer.registerMessageListener(messageListenerConcurrently);
        } else if (listenerOrderly != null) {
            consumer.registerMessageListener(listenerOrderly);
        }
        consumer.start();
        ConsumerIndex consumerIndex = new ConsumerIndex();
        consumerIndex.setConsumerConfig(config);
        consumerIndex.setIndex(index);
        consumerContainers.put(consumerIndex, consumer);
    }

    private void configureConsumerForStability(DefaultMQPushConsumer consumer) {
        // 减少路由拉取间隔
         consumer.setPollNameServerInterval(10000); // 10秒
        // 设置心跳间隔
        consumer.setHeartbeatBrokerInterval(10000); // 10秒
        // 设置消费线程数
        consumer.setConsumeThreadMin(5);
        consumer.setConsumeThreadMax(10);
        // 设置消费超时
        consumer.setConsumeTimeout(15); // 15分钟
    }


    private void shutdownConsumer(ConsumerIndex index) {
        MQPushConsumer mqPushConsumer = consumerContainers.get(index);
        if (mqPushConsumer != null) {
            mqPushConsumer.shutdown();
            consumerContainers.remove(mqPushConsumer);
            log.info("消费者已关闭: {}", mqPushConsumer);
        }
    }

    @PreDestroy
    public void destroy() {
        log.info("正在关闭所有 RocketMQ 消费者...");
        consumerContainers.values().forEach(consumer -> {
            consumer.shutdown();
        });
        consumerContainers.clear();
    }
}
