package com.airfree.rpc.core;

import com.airfree.discovery.AirGatewayDiscoveryService;
import com.airfree.discovery.discoveryServiceImpl.AirGatewayNacosDiscoveryServiceImpl;
import com.airfree.rpc.AirGatewayRPCServiceClient;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class AirRPCClientFactory {

    private final ApplicationContext applicationContext;
    private final Map<AirGatewayDiscoveryService, List<AirGatewayRPCServiceClient>> rpcInstanceRelationMap;


    public AirRPCClientFactory(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        //获取所有的AirGatewayDiscoveryService
        Map<String, AirGatewayDiscoveryService> discoveryServiceMap = applicationContext.getBeansOfType(AirGatewayDiscoveryService.class);
        //获取所有的AirGatewayRPCServiceClient
        Map<String, AirGatewayRPCServiceClient> rpcServiceClientMap = applicationContext.getBeansOfType(AirGatewayRPCServiceClient.class);
        //构建discoveryServiceMap与rpcServiceClientMap的关系
        this.rpcInstanceRelationMap = buildRelation(discoveryServiceMap, rpcServiceClientMap);
        log.info("####AirRPCClientFactory初始化成功--->rpcInstanceRelationMap信息为:{}", this.rpcInstanceRelationMap);
    }

    private Map<AirGatewayDiscoveryService, List<AirGatewayRPCServiceClient>> buildRelation(Map<String, AirGatewayDiscoveryService> discoveryServiceMap,
                                                                                            Map<String, AirGatewayRPCServiceClient> rpcServiceClientMap) {
        Map<AirGatewayDiscoveryService, List<AirGatewayRPCServiceClient>> rpcInstanceRelationMap = new ConcurrentHashMap<>();
        discoveryServiceMap.entrySet().forEach(entry -> {
            AirGatewayDiscoveryService discoveryService = entry.getValue();
            List<AirGatewayRPCServiceClient> initList = new ArrayList<>();
            List<AirRPCTypeEnum> airRPCTypeEnums = entry.getValue().supportRpcType();
            airRPCTypeEnums.forEach(e -> {
                rpcServiceClientMap.entrySet().forEach(clientEntry -> {
                    if (clientEntry.getValue().getProtocol().equals(e.getRpcName())) {
                        initList.add(clientEntry.getValue());
                    }
                });
            });
            rpcInstanceRelationMap.put(discoveryService, initList);
        });
        return rpcInstanceRelationMap;
    }

    private AirGatewayDiscoveryService findTargetRpcService(String serviceDiscoveryName) {
        AtomicReference<AirGatewayDiscoveryService> discoveryService = null;
        rpcInstanceRelationMap.entrySet().forEach(entry -> {
            if (serviceDiscoveryName.equals(entry.getKey().getDiscoveryName())) {
                discoveryService.set(entry.getKey());
            }
        });
        return discoveryService.get();
    }

    private AirGatewayRPCServiceClient findMatchRpcClient(AirGatewayDiscoveryService discoveryService, String protocol) {
        AirGatewayRPCServiceClient clientInstance = null;
        List<AirGatewayRPCServiceClient> airGatewayRPCServiceClients = rpcInstanceRelationMap.get(discoveryService);
        for (AirGatewayRPCServiceClient client : airGatewayRPCServiceClients) {
            if (protocol.equals(client.getProtocol())) {
                clientInstance = client;
            }
        }
        return clientInstance;
    }

    public Mono<AirGatewayRPCServiceClient> getClientForService(String serviceDiscoveryName, String protocol, String serviceName) throws RuntimeException {

        AirGatewayDiscoveryService discoveryService = findTargetRpcService(serviceDiscoveryName);
        AirGatewayRPCServiceClient matchRpcClient = findMatchRpcClient(discoveryService, protocol);

        return discoveryService.resolveEndpoint(serviceName, protocol)
                .map(endpoint -> {
                    if (matchRpcClient != null && matchRpcClient.support(endpoint)) {
                        return matchRpcClient;
                    } else {
                        throw new RuntimeException("Unsupported protocol: " + protocol + " for service: " + serviceName);
                    }
                });
    }

}
