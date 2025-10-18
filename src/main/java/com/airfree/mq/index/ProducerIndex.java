package com.airfree.mq.index;

import com.airfree.mq.cofig.rocketMq.ProducerConfig;
import lombok.Data;

@Data
public class ProducerIndex {
    private ProducerConfig consumerConfig;
    private int index;
}
