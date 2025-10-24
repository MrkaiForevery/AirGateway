package com.airfree.discovery.discoveryService;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.enums.AirServiceDiscoveryTypeEnum;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Properties;

@Slf4j
public class AirNacosReactiveDiscovery implements AirReactiveServiceDiscovery {

    private NamingService namingService;
    private AirRegistryCenterConfig config;
    private final Sinks.Many<AirServiceInstanceEvent> eventSink =  Sinks.many().multicast().onBackpressureBuffer(1000);

    @Override
    public String getRegistryType() {
        return AirServiceDiscoveryTypeEnum.NACOS_DISCOVERY.getTypeName();
    }

    @Override
    public Mono<Void> initialize(AirRegistryCenterConfig config) {
        this.config = config;
        return Mono.fromCallable(() -> {

            Properties properties = new Properties();
            properties.put("serverAddr", String.join(",", config.getServerAddresses()));
            properties.put("namespace", config.getNamespace());
            properties.put("group",config.getGroup());
            properties.put(" clusterName",config.getClusterName());

            if (config.getUsername() != null) {
                properties.put("username", config.getUsername());
                properties.put("password", config.getPassword());
            }

            this.namingService = NamingFactory.createNamingService(properties);
            return namingService;
        }).then();

    }

    @Override
    public Flux<String> getServiceNames() {
        return Mono.fromCallable(() -> namingService.getServicesOfServer(1, Integer.MAX_VALUE).getData())
                .flatMapMany(Flux::fromIterable)
                .onErrorResume(e -> {
                    log.error("获取服务列表失败", e);
                    return Flux.empty();
                });
    }

    @Override
    public Flux<AirServiceInstance> getInstances(String serviceId) {
        return Mono.fromCallable(() -> namingService.getAllInstances(serviceId))
                .flatMapMany(Flux::fromIterable)
                .map(this::convertToServiceInstance)
                .onErrorResume(e -> {
                    log.error("获取服务实例失败: {}", serviceId, e);
                    return Flux.empty();
                });
    }

    private AirServiceInstance convertToServiceInstance(Instance instance) {
        AirServiceInstance si = new AirServiceInstance();
        si.setInstanceId(instance.getInstanceId());
        si.setServiceId(instance.getServiceName());
        si.setHost(instance.getIp());
        si.setPort(instance.getPort());
        si.setSecure("https".equals(instance.getMetadata().get("secure")));
        si.setMetadata(instance.getMetadata());
        return si;
    }

    @Override
    public Flux<AirServiceInstanceEvent> subscribe(String serviceId) {
        return Mono.fromRunnable(() -> {
            try {
                namingService.subscribe(serviceId, event -> {
                    if (event instanceof NamingEvent) {
                        handleNamingEvent(serviceId, (NamingEvent) event);
                    }
                });
            } catch (Exception e) {
                log.error("订阅服务失败: {}", serviceId, e);
            }
        }).thenMany(eventSink.asFlux().filter(event -> serviceId.equals(event.getServiceId())));
    }

    private void handleNamingEvent(String serviceId, NamingEvent event) {
        List<Instance> instances = event.getInstances();

        for (Instance instance : instances) {
            AirServiceInstance serviceInstance = convertToServiceInstance(instance);
            AirServiceInstanceEvent instanceEvent = new AirServiceInstanceEvent(
                    serviceId, serviceInstance, AirServiceInstanceEvent.EventType.ADDED
            );
            eventSink.tryEmitNext(instanceEvent);
        }
    }


    @Override
    public Mono<Boolean> isHealthy() {
        return Mono.fromCallable(() -> "UP".equals(namingService.getServerStatus()));
    }
}
