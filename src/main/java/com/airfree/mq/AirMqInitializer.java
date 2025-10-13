package com.airfree.mq;

import org.springframework.stereotype.Component;


public interface AirMqInitializer {

    void creteProducer();
    void removeProducer();
    void createConsumer();
    void removerConsumer();

}
