package com.airfree;

import com.airfree.mq.cofig.rocketMq.AirRocketMQConfigProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableConfigurationProperties(AirRocketMQConfigProperties.class)
@EnableDiscoveryClient
@EnableAspectJAutoProxy
@SpringBootApplication
public class AirGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(AirGatewayApplication.class);
    }
}