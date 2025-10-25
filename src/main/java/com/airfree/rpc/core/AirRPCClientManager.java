package com.airfree.rpc.core;

import com.airfree.router.core.AirMultiRegistryRouteDefinitionLocator;
import com.airfree.router.tools.AirRouteMatcher;
import com.airfree.rpc.AirGatewayRPCServiceClient;
import com.airfree.rpc.entity.AirRouteRPCContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Component
public class AirRPCClientManager {

    private final AirMultiRegistryRouteDefinitionLocator routeLocator;
    private final Map<String, AirGatewayRPCServiceClient> protocolClients = new ConcurrentHashMap<>();
    private final Map<String, AirRouteRPCContext> routeClients = new ConcurrentHashMap<>();
    private final AirRouteMatcher routeMatcher;

    public AirRPCClientManager(AirMultiRegistryRouteDefinitionLocator routeLocator,
                               Map<String, AirGatewayRPCServiceClient> rpcClients,
                               AirRouteMatcher routeMatcher) {
        this.routeLocator = routeLocator;
        this.routeMatcher = routeMatcher;
        // 初始化协议客户端映射
        rpcClients.forEach((beanName, client) -> {
            protocolClients.put(client.getProtocol(), client);
            log.info("注册RPC客户端: {} -> {}", client.getProtocol(), client.getClass().getSimpleName());
        });
        // 监听路由变化，动态管理RPC客户端
        initializeRouteClients();
    }

    /**
     * 初始化路由客户端
     */
    private void initializeRouteClients() {
        routeLocator.getRouteDefinitions()
                .subscribe(route -> {
                    AirRouteRPCContext context = createRouteRPCContext(route);
                    routeClients.put(route.getId(), context);
                    log.debug("为路由 {} 创建RPC上下文", route.getId());
                });
    }

    /**
     * 创建路由RPC上下文
     */
    private AirRouteRPCContext createRouteRPCContext(RouteDefinition route) {
        String protocol = extractProtocolFromRoute(route);
        AirGatewayRPCServiceClient client = protocolClients.get(protocol);

        if (client == null) {
            log.warn("路由 {} 使用不支持的协议: {}", route.getId(), protocol);
            return new AirRouteRPCContext(route, null, protocol);
        }

        return new AirRouteRPCContext(route, client, protocol);
    }

    /**
     * 从路由中提取协议
     */
    private String extractProtocolFromRoute(RouteDefinition route) {
        // 方案1: 从URI scheme提取
        String scheme = route.getUri().getScheme();
        if (!"lb".equals(scheme)) {
            return scheme; // 直接协议，如 http, https, grpc
        }

        // 方案2: 从元数据提取
        Map<String, Object> metadata = route.getMetadata();
        if (metadata != null && metadata.containsKey("rpc.protocol")) {
            return (String) metadata.get("rpc.protocol");
        }

        // 方案3: 从过滤器配置提取 todo 这个待实现
//        return extractProtocolFromFilters(route.getFilters());
        return null;
    }

    /**
     * 根据路由ID获取RPC客户端
     */
    public Mono<AirGatewayRPCServiceClient> getClientForRoute(String routeId) {
        return Mono.fromCallable(() -> {
            AirRouteRPCContext context = routeClients.get(routeId);
            if (context == null) {
                throw new IllegalArgumentException("未找到路由: " + routeId);
            }

            if (context.getClient() == null) {
                throw new IllegalArgumentException("路由 " + routeId + " 没有可用的RPC客户端");
            }

            return context.getClient();
        });
    }

    /**
     * 根据请求路径获取RPC客户端
     */
    public Mono<AirGatewayRPCServiceClient> getClientForPath(String path,ServerWebExchange exchangeArg) {
        return routeLocator.getRouteDefinitions()
                .filter(route -> matchesRoute(route, path,exchangeArg))
                .next()
                .flatMap(route -> getClientForRoute(route.getId()));
    }

    /**
     * 检查路径是否匹配路由
     */
    private boolean matchesRoute(RouteDefinition route, String path,ServerWebExchange exchangeArg) {
        // 这里需要实现路由匹配逻辑,todo 这里先临时这样写,这个exchange先整成null
        return routeMatcher.matches(route, path, exchangeArg);

    }
}