package com.airfree.mq.cofig.kafka;

import lombok.Data;

@Data
public class KafkaProducerConfig {

    private String bootstrapServers;
    private String keySerializer;
    private String valueSerializer;
    private long batchSize;
    private String ack;
    private long bufferMemory;
}
