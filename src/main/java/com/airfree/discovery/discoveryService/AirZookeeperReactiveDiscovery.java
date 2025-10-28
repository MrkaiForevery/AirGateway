package com.airfree.discovery.discoveryService;

import com.airfree.discovery.AirReactiveServiceDiscovery;
import com.airfree.discovery.config.AirRegistryCenterConfig;
import com.airfree.discovery.enums.AirServiceDiscoveryTypeEnum;
import com.airfree.discovery.event.AirServiceInstanceEvent;
import com.airfree.discovery.instance.AirServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AirZookeeperReactiveDiscovery implements AirReactiveServiceDiscovery {

    private CuratorFramework client;
    private boolean initialized = false;
    private volatile boolean closed = false;

    @Override
    public String getRegistryType() {
        return AirServiceDiscoveryTypeEnum.ZOOKEEPER_DISCOVERY.getTypeName();
    }

    @Override
    public Mono<Void> initialize(AirRegistryCenterConfig config) {
        return Mono.fromRunnable(() -> {
            try {
                this.client = CuratorFrameworkFactory.newClient( String.join(",", config.getServerAddresses()),
                        new ExponentialBackoffRetry(1000, 3));
                client.start();
                client.blockUntilConnected(30, TimeUnit.SECONDS);
                createBasePaths();
                initialized = true;
                log.info("Zookeeper 客户端初始化成功: {}", config.getServerAddresses());
            } catch (Exception e) {
                throw new RuntimeException("Zookeeper 初始化失败", e);
            }
        });
    }

    @Override
    public Flux<String> getServiceNames() {
        if (!initialized) {
            return Flux.error(new IllegalStateException("zookeeper 客户端未初始化"));
        }
        return Flux.defer(() -> {
            try {
                // 双重检查路径是否存在
                if (client.checkExists().forPath("/services") == null) {
                    log.warn("Zookeeper 路径 /services 不存在，可能是初始化问题");
                    return Flux.empty();
                }

                List<String> services = client.getChildren().forPath("/services");
                return Flux.fromIterable(services);
            } catch (Exception e) {
                return Flux.error(e);
            }
        });
    }

    @Override
    public Flux<AirServiceInstance> getInstances(String serviceId) {
        if (!initialized) {
            return Flux.error(new IllegalStateException("zookeeper 客户端未初始化"));
        }
        // 直接返回原始数据，缓存由管理器处理
        return Flux.defer(() -> {
            try {
                String path = "/services/" + serviceId + "/instances";
                List<String> instanceIds = client.getChildren().forPath(path);

                List<AirServiceInstance> instances = new ArrayList<>();
                for (String instanceId : instanceIds) {
                    byte[] data = client.getData().forPath(path + "/" + instanceId);
                    AirServiceInstance instance = parseInstanceData(serviceId, instanceId, data);
                    if (instance != null) {
                        instances.add(instance);
                    }
                }
                return Flux.fromIterable(instances);
            } catch (Exception e) {
                return Flux.error(e);
            }
        });
    }

    @Override
    public Flux<AirServiceInstanceEvent> subscribe(String serviceId) {
        if (closed) {
            return Flux.error(new IllegalStateException("Zookeeper 客户端已关闭"));
        }
        if (!initialized) {
            return Flux.error(new IllegalStateException("zookeeper 客户端未初始化"));
        }
        // 返回原始事件流，事件处理由管理器负责
        return Flux.create(sink -> {
            try {
                String path = "/services/" + serviceId + "/instances";
                CuratorCache cache = CuratorCache.build(client, path);

                CuratorCacheListener listener = (type, oldData, newData) -> {
                    AirServiceInstanceEvent event = createEventFromZkData(type, serviceId, newData);
                    if (event != null) {
                        sink.next(event);
                    }
                };

                cache.listenable().addListener(listener);
                cache.start();

                sink.onCancel(() -> {
                    cache.close();
                });

            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Boolean> isHealthy() {
        return Mono.fromCallable(() ->
                initialized && client != null && client.getZookeeperClient().isConnected()
        );
    }

    @Override
    public Mono<Boolean> registerInstance(String serviceName, String host, int port, Map<String, String> metadata) {
        return Mono.fromCallable(() -> {
            String instancePath = "/services/" + serviceName + "/instances/" + host + ":" + port;

            // 创建实例节点数据
            String instanceData = buildInstanceData(host, port, metadata);

            // 创建临时节点（会话结束后自动删除）
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(instancePath, instanceData.getBytes());

            log.info("Zookeeper - 服务实例注册成功: {} - {}:{}", serviceName, host, port);
            return true;
        }).onErrorResume(e -> {
            log.error("Zookeeper - 注册服务实例失败: {} - {}:{}", serviceName, host, port, e);
            return Mono.just(false);
        });
    }

    @Override
    public Mono<Boolean> deregisterInstance(String serviceName, String host, int port) {
        return Mono.fromCallable(() -> {
            String instancePath = "/services/" + serviceName + "/instances/" + host + ":" + port;

            if (client.checkExists().forPath(instancePath) != null) {
                client.delete().forPath(instancePath);
                log.info("Zookeeper - 服务实例注销成功: {} - {}:{}", serviceName, host, port);
            }
            return true;
        }).onErrorResume(e -> {
            log.error("Zookeeper - 注销服务实例失败: {} - {}:{}", serviceName, host, port, e);
            return Mono.just(false);
        });
    }

    @Override
    public boolean supportsRegistration() {
        return true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        log.info("开始关闭 Zookeeper 发现客户端...");

        try {
            //  关闭 CuratorFramework 客户端
            if (client != null) {
                client.close();
                log.debug("Zookeeper - CuratorFramework 已关闭");
            }

            // 3. 更新状态
            closed = true;
            initialized = false;

            log.info("Zookeeper 发现客户端关闭完成");

        } catch (Exception e) {
            log.warn("关闭 Zookeeper 发现客户端时发生异常", e);
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    // ==================== 私有方法 ====================
    private void createBasePaths() throws Exception {
        // 创建基础路径，todo 这里先写死
        try {
            // 创建基础服务目录
            createPathIfNotExists("/services");
            log.debug("Zookeeper 基础路径创建完成");

        } catch (Exception e) {
            log.warn("创建基础路径失败，但可能路径已存在: {}", e.getMessage());
            // 不抛出异常，因为路径可能已经存在
        }
    }

    private void createPathIfNotExists(String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            // 创建持久化节点
            client.create().creatingParentsIfNeeded().forPath(path);
            log.debug("创建路径: {}", path);
        }
    }

    private AirServiceInstance parseInstanceData(String serviceId, String instanceId, byte[] data) {
        // 解析实例数据
        return AirServiceInstance.builder()
                .serviceId(serviceId)
                .instanceId(instanceId)
                .host("parsed-host")
                .port(8080)
                .metadata(new HashMap<>())
                .build();
    }

    private AirServiceInstanceEvent createEventFromZkData(
            CuratorCacheListener.Type type, String serviceId,
            org.apache.curator.framework.recipes.cache.ChildData data) {
        // 创建原始事件
        return null; // 简化实现
    }

    private String buildInstanceData(String host, int port, Map<String, String> metadata) {
        Map<String, Object> data = new HashMap<>();
        data.put("host", host);
        data.put("port", port);
        data.put("timestamp", System.currentTimeMillis());

        if (metadata != null) {
            data.putAll(metadata);
        }

        // 简化实现，实际应该使用 JSON
        return "host=" + host + "&port=" + port + "&timestamp=" + System.currentTimeMillis();
    }
}
