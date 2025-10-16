package com.airfree;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableDiscoveryClient
@EnableAspectJAutoProxy
@SpringBootApplication
public class AirGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(AirGatewayApplication.class);
    }
}