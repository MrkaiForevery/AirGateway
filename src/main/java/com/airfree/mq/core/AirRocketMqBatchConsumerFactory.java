package com.airfree.mq.core;

import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import com.airfree.mq.cofig.rocketMq.ConsumerConfig;
import com.airfree.mq.index.ConsumerIndex;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AirRocketMqBatchConsumerFactory {

    private final AirRocketMQConfigProperties rocketMQConfigProperties;
    private final ApplicationContext applicationContext;
    private final Map<ConsumerIndex, DefaultRocketMQListenerContainer> consumerContainers = new ConcurrentHashMap<>();

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

    //精确控制某个instance的重新构建和启动
    public synchronized int startContainerInstancePreciseByRebuildInstance(ConsumerIndex index){
        AtomicInteger atomicInteger = new AtomicInteger(0);
        this.consumerContainers.entrySet().forEach(entry ->{
            if (index.isSameConsumerInstance(entry.getKey())) {
                if (entry.getValue().isRunning()) {
                    log.info("consumer:{},index:{} 正在运行中，正在进行关闭....",entry.getValue(),index.getIndex());
                    try {
                        shutDownContainerInstancePrecise(index);
                        log.info("consumer:{},index:{} 已关闭！！！！",entry.getValue(),index.getIndex());
                    }catch (Exception e){
                        atomicInteger.set(-1);
                        log.info("consumer:{},index:{} 关闭失败！！！！",entry.getValue(),index.getIndex());
                    }
                }
                try {
                    log.info("consumer:{},index:{} 正在重新构建....", entry.getValue(), index.getIndex());
                    createSingleConsumerInstanceAndStartIt(index.getConsumerConfig(), index.getIndex());
                    log.info("consumer:{},index:{} 已重新构建并启动!!!", entry.getValue(), index.getIndex());
                    atomicInteger.set(1);
                }catch (Exception e){
                    atomicInteger.set(-1);
                    log.info("consumer:{},index:{} 重新构建失败!!!", entry.getValue(), index.getIndex());
                }
            }
        });
        return atomicInteger.get();
    }

    //精确控制某个instance的停止
    public synchronized int shutDownContainerInstancePrecise(ConsumerIndex index){
        AtomicInteger flag = new AtomicInteger(0);
        this.consumerContainers.entrySet().forEach(entry ->{
            if (index.isSameConsumerInstance(entry.getKey())) {
                if (entry.getValue().isRunning()) {
                    log.info("consumer:{},index:{} 正在运行中，正在进行关闭...",entry.getValue(),index.getIndex());
                    try {
                        shutdownConsumer(index);
                        flag.set(1);
                        log.info("consumer:{},index:{} 关闭成功！！！",entry.getValue(),index.getIndex());
                    }catch (Exception e){
                        flag.set(-1);
                        log.info("consumer:{},index:{} 关闭失败!!!", entry.getValue(), index.getIndex());
                    }
                }
            }
        });
        return flag.get();
    }


    public synchronized void buildOneTypeConsumer(ConsumerConfig config) throws ClassNotFoundException {
        Integer instanceNums = 1;
        if (config.getInstanceNums() != null) {
            instanceNums = Integer.valueOf(config.getInstanceNums());
        }
        for (int i = 0; i < instanceNums; i++) {
            createSingleConsumerInstanceAndStartIt(config, i);
        }
        log.info("构建同一消费者实例成功！。。。。");
    }

    private void createSingleConsumerInstanceAndStartIt(ConsumerConfig config, int index) throws ClassNotFoundException {
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
        DefaultRocketMQListenerContainer container = buildDefaultRocketMQListenerContainer(rocketMQConfigProperties.getNameServer(), config.getGroup(), config.getTopic(), config.getTags());
        // todo 设置消费者参数
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(config.getGroup());
        consumer.setNamesrvAddr(rocketMQConfigProperties.getNameServer());
        // 注册消费者的回调接口处理消息
        if (messageListenerConcurrently != null) {
            consumer.registerMessageListener(messageListenerConcurrently);
        } else if (listenerOrderly != null) {
            consumer.registerMessageListener(listenerOrderly);
        }
        container.setConsumer(consumer);
        container.start();
        ConsumerIndex consumerIndex = new ConsumerIndex();
        consumerIndex.setConsumerConfig(config);
        consumerIndex.setIndex(index);
        consumerContainers.put(consumerIndex, container);
    }

    private DefaultRocketMQListenerContainer buildDefaultRocketMQListenerContainer(String nameServer, String consumerGroup, String topic, Map<String, String> tags) {

        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();
        container.setNameServer(nameServer);
        container.setConsumerGroup(consumerGroup);
        container.setTopic(topic);
        StringBuilder selectorExpression = new StringBuilder();
        if (tags.size() > 0) {
            List<String> tagList = tags.values().stream().collect(Collectors.toList());
            for (int i = 0; i < tagList.size(); i++) {
                selectorExpression.append(tagList.get(i));
                if (tagList.get(i) != null) {
                    selectorExpression.append("||");
                }
            }
        }
        if ( selectorExpression.toString() != null && selectorExpression.toString() != "" ) {
            container.setSelectorExpression(selectorExpression.toString());
        }
        return container;
    }

    private void shutdownConsumer(ConsumerIndex index) {
        DefaultRocketMQListenerContainer container = consumerContainers.get(index);
        if (container != null) {
            container.getConsumer().shutdown();
            consumerContainers.remove(container);
            log.info("消费者已关闭: {}", container);
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
