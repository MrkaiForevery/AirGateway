package com.airfree.log.config;

import com.airfree.aspect.AirGatewayLogAspect;
import com.airfree.filter.globalFilter.AirGatewayLogFilter;
import com.airfree.log.core.AirGatewayLogPublisher;
import com.airfree.log.core.storage.AirGatewayLogStorage;
import com.airfree.log.core.storage.DefaultAirGatewayLogStorage;
import com.airfree.monitor.actuator.AirGatewayJavaClock;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AirGatewayLogProperties.class)
public class AirGatewayLogAutoConfiguration {


    @Bean
    @ConditionalOnMissingBean
    public AirGatewayLogFilter gatewayLogFilter(AirGatewayLogPublisher logPublisher,
                                                AirGatewayLogProperties properties) {
        return new AirGatewayLogFilter(logPublisher, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public AirGatewayLogPublisher gatewayLogPublisher(AirGatewayLogStorage logStorage,
                                                      AirGatewayLogProperties properties,
                                                      MeterRegistry meterRegistry,
                                                      ApplicationEventPublisher eventPublisher,
                                                      AirGatewayJavaClock clock) {
        return new AirGatewayLogPublisher(logStorage, properties, meterRegistry, eventPublisher,clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public AirGatewayLogStorage gatewayLogStorage(AirGatewayLogProperties properties) {
        return new DefaultAirGatewayLogStorage(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public AirGatewayLogAspect gatewayLogAspect(AirGatewayLogPublisher logPublisher,MeterRegistry meterRegistry) {
        return new AirGatewayLogAspect(logPublisher,meterRegistry);
    }
}
