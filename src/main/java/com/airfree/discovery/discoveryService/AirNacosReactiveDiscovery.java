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
import com.alibaba.nacos.api.naming.pojo.ListView;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AirNacosReactiveDiscovery implements AirReactiveServiceDiscovery {

    private NamingService namingService;
    private final Map<String, Sinks.Many<AirServiceInstanceEvent>> eventSinks = new ConcurrentHashMap<>();
    private boolean initialized = false;
    private volatile boolean closed = false;

    @Override
    public String getRegistryType() {
        return AirServiceDiscoveryTypeEnum.NACOS_DISCOVERY.getTypeName();
    }

    @Override
    public Mono<Void> initialize(AirRegistryCenterConfig config) {
        return Mono.fromRunnable(() -> {
            try {
                Properties properties = new Properties();
                properties.put("serverAddr", String.join(",", config.getServerAddresses()));
                properties.put("namespace", config.getNamespace());
                properties.put("group", config.getGroup());

                if (config.getUsername() != null) {
                    properties.put("username", config.getUsername());
                    properties.put("password", config.getPassword());
                }

                this.namingService = NamingFactory.createNamingService(properties);
                initialized = true;
                log.info("Nacos 客户端初始化成功: {}", config.getServerAddresses());
            } catch (Exception e) {
                log.error("Nacos 客户端初始化失败", e);
                throw new RuntimeException("Nacos 初始化失败", e);
            }
        });
    }

    @Override
    public Flux<String> getServiceNames() {
        if (closed) {
            return Flux.error(new IllegalStateException("Nacos 客户端已关闭"));
        }
        if (!initialized) {
            return Flux.error(new IllegalStateException("Nacos 客户端未初始化"));
        }
        return Mono.fromCallable(() -> {
                    // 获取所有服务
                    ListView<String> services = namingService.getServicesOfServer(1, Integer.MAX_VALUE);
                    log.info("Nacos 中所有服务: {}", services.getData());
                    return services.getData();
                })
                .flatMapMany(Flux::fromIterable)
                .onErrorResume(e -> {
                    log.error("获取服务列表失败", e);
                    return Flux.empty();
                });
    }

    @Override
    public Flux<AirServiceInstance> getInstances(String serviceId) {
        if (!initialized) {
            return Flux.error(new IllegalStateException("Nacos 客户端未初始化"));
        }
        return Mono.fromCallable(() -> namingService.getAllInstances(serviceId))
                .flatMapMany(Flux::fromIterable)
                .map(this::convertToServiceInstance)
                .onErrorResume(e -> {
                    log.error("获取服务实例失败: {}", serviceId, e);
                    return Flux.empty();
                });
    }

    @Override
    public Flux<AirServiceInstanceEvent> subscribe(String serviceId) {
        if (!initialized) {
            return Flux.error(new IllegalStateException("Nacos 客户端未初始化"));
        }
        return Flux.create(sink -> {
            try {
                // 为每个服务创建独立的事件流
                Sinks.Many<AirServiceInstanceEvent> serviceSink = Sinks.many().multicast().onBackpressureBuffer();
                eventSinks.put(serviceId, serviceSink);

                // 订阅 Nacos 事件
                namingService.subscribe(serviceId, event -> {
                    if (event instanceof NamingEvent) {
                        handleNamingEvent(serviceId, (NamingEvent) event);
                    }
                });

                // 返回事件流
                serviceSink.asFlux().subscribe(sink::next, sink::error, sink::complete);

                sink.onCancel(() -> {
                    eventSinks.remove(serviceId);
                    serviceSink.tryEmitComplete();
                });

            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Boolean> isHealthy() {
        return Mono.fromCallable(() ->
                initialized && "UP".equals(namingService.getServerStatus())
        );
    }

    /**
     * 注册服务实例到 Nacos ---> 用于把自己注册到nacos里面
     */
    @Override
    public Mono<Boolean> registerInstance(String serviceName, String ip, int port,
                                          Map<String, String> metadata) {
        return Mono.fromCallable(() -> {
            Instance instance = new Instance();
            instance.setIp(ip);
            instance.setPort(port);
            instance.setServiceName(serviceName);
            instance.setWeight(1.0);
            instance.setHealthy(true);
            instance.setEnabled(true);

            if (metadata != null) {
                instance.setMetadata(metadata);
            }

            namingService.registerInstance(serviceName, instance);
            log.info("服务实例注册成功: {} - {}:{}", serviceName, ip, port);
            return true;
        }).onErrorResume(e -> {
            log.error("注册服务实例失败: {} - {}:{}", serviceName, ip, port, e);
            return Mono.just(false);
        });
    }


    /**
     * 注销服务实例
     */
    @Override
    public Mono<Boolean> deregisterInstance(String serviceName, String ip, int port) {
        return Mono.fromCallable(() -> {
            namingService.deregisterInstance(serviceName, ip, port);
            log.info("服务实例注销成功: {} - {}:{}", serviceName, ip, port);
            return true;
        }).onErrorResume(e -> {
            log.error("注销服务实例失败: {} - {}:{}", serviceName, ip, port, e);
            return Mono.just(false);
        });
    }

    @Override
    public boolean supportsRegistration() {
        return true;
    }


    // ==================== 私有辅助方法 ====================
    private AirServiceInstance convertToServiceInstance(Instance instance) {
        return AirServiceInstance.builder()
                .serviceId(instance.getServiceName())
                .instanceId(instance.getInstanceId())
                .host(instance.getIp())
                .port(instance.getPort())
                .secure("https".equals(instance.getMetadata().get("secure")))
                .metadata(instance.getMetadata())
                .build();
    }

    private void handleNamingEvent(String serviceId, NamingEvent event) {
        Sinks.Many<AirServiceInstanceEvent> sink = eventSinks.get(serviceId);
        if (sink == null) return;

        List<Instance> instances = event.getInstances();

        // 简化的处理逻辑：只发布服务级别的事件
        // 具体实例级别的变化由管理器处理
        for (Instance instance : instances) {
            AirServiceInstance serviceInstance = convertToServiceInstance(instance);

            // 这里可以简化事件类型，让管理器去判断具体的变化
            AirServiceInstanceEvent instanceEvent = new AirServiceInstanceEvent(
                    serviceInstance.getServiceId(),
                    serviceInstance,
                    AirServiceInstanceEvent.EventType.MODIFIED // 使用统一的事件类型
            );

            sink.tryEmitNext(instanceEvent);
        }
    }

    // 资源清理
    @Override
    public void close() {
        if (closed) {
            return;
        }
        log.info("开始关闭 Nacos 发现客户端...");
        try {
            // 1. 先关闭所有事件流
            eventSinks.forEach((serviceId, sink) -> {
                sink.tryEmitComplete();
                log.debug("Nacos - 关闭服务 {} 的事件流", serviceId);
            });
            eventSinks.clear();

            // 2. 关闭 NamingService
            if (namingService != null) {
                namingService.shutDown();
                log.debug("Nacos - NamingService 已关闭");
            }

            // 3. 更新状态
            closed = true;
            initialized = false;

            log.info("Nacos 发现客户端关闭完成");

        } catch (Exception e) {
            log.warn("关闭 Nacos 发现客户端时发生异常", e);
            closed = true; // 即使出错也标记为已关闭
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
