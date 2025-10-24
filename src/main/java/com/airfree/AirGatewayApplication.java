package com.airfree;

import com.airfree.cache.config.mongo.AirMongoDbConfigProperties;
import com.airfree.cache.config.redis.AirRedissonConfigProperties;
import com.airfree.discovery.config.MultiAirRegistryCenterConfig;
import com.airfree.flow.config.AirFlowControlConfigProperties;
import com.airfree.mq.cofig.kafka.AirKafkaConfigProperties;
import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableConfigurationProperties({
        AirRocketMQConfigProperties.class,
        AirRedissonConfigProperties.class,
        AirMongoDbConfigProperties.class,
        AirFlowControlConfigProperties.class,
        AirKafkaConfigProperties.class,
        MultiAirRegistryCenterConfig.class
})
@EnableDiscoveryClient
@EnableAspectJAutoProxy
@SpringBootApplication
public class AirGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(AirGatewayApplication.class);
    }
}