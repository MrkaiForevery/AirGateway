package com.airfree.mq.index;

import com.airfree.mq.cofig.rocketMq.ConsumerConfig;
import lombok.Data;

@Data
public class ConsumerIndex {
    private ConsumerConfig consumerConfig;
    private int index;

    public boolean isSameTypeConsumerInstance(ConsumerConfig config) {
        if (config.getGroup().equals(this.consumerConfig.getGroup())
                && config.getTopic().equals(this.consumerConfig.getTopic())) {
            return true;
        }
        return false;
    }

    public boolean isSameConsumerInstance(ConsumerIndex index) {
        if (index.getConsumerConfig().getGroup().equals(this.consumerConfig.getGroup())
                && index.getConsumerConfig().getTopic().equals(this.consumerConfig.getTopic())
                && index.getIndex() == this.index) {
            return true;
        }
        return false;
    }
}
