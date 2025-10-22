package com.airfree.mq.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class AirGenericMessageConcurrentlyListener implements MessageListenerConcurrently {

    private final String topic;

    public AirGenericMessageConcurrentlyListener() {
        this.topic = "topic_TIMEOUT_topic";
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

        log.info("已接收到消息{}",msgs);
        //默认消费消息成功，需要给ack给到rocketmq
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
