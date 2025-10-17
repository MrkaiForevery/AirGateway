package com.airfree.discovery.discoveryServiceImpl;

import com.airfree.discovery.AirGatewayDiscoveryService;
import com.airfree.rpc.entity.AirServiceEndpoint;
import com.airfree.rpc.enums.AirRPCTypeEnum;
import com.alibaba.cloud.nacos.discovery.NacosDiscoveryClient;
import com.alibaba.cloud.nacos.discovery.NacosServiceDiscovery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class AirGatewayNacosDiscoveryServiceImpl implements AirGatewayDiscoveryService {

    private final NacosDiscoveryClient discoveryClient;
    private final ApplicationContext applicationContext;
    private final List<AirRPCTypeEnum> supportRpcTypes;

    public AirGatewayNacosDiscoveryServiceImpl(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        //todo 这里这样只能这样new一个，使用@ConditionalOnBean(NacosDiscoveryClient.class)没有这个bean
        NacosServiceDiscovery nacosServiceDiscovery = this.applicationContext.getBeansOfType(NacosServiceDiscovery.class).get("nacosServiceDiscovery");
        this.discoveryClient = new NacosDiscoveryClient(nacosServiceDiscovery);
        List<AirRPCTypeEnum> prcTypeList = new ArrayList<>();
        prcTypeList.add(AirRPCTypeEnum.RSOCKET);
        prcTypeList.add(AirRPCTypeEnum.GRPC);
        prcTypeList.add(AirRPCTypeEnum.HTTP);
        this.supportRpcTypes = prcTypeList;
        log.info("################加载的NacosDiscoveryClient信息为:{}", this.discoveryClient);
    }


    @Override
    public Mono<AirServiceEndpoint> resolveEndpoint(String serviceName, String protocol) {
        return Mono.empty();
    }

    @Override
    public String getDiscoveryName() {
        return "nacos";
    }

    @Override
    public List<AirRPCTypeEnum> supportRpcType() {
        return this.supportRpcTypes;
    }
}
