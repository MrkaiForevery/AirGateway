package com.airfree.filter.filterManager;

import com.airfree.filter.AbstractAirGatewayFilter;
import com.airfree.filter.AirGatewayFilterManager;
import com.airfree.filter.AirGatewayStrategy;
import com.airfree.filter.filterChain.CustomWebFilterChain;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.server.WebHandler;
import org.springframework.web.server.handler.DefaultWebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

//todo 这里应该要添加一个监听器，同步刷新策略缓存cache
@Slf4j
@Component
public class DefaultAirGatewayFilterManager implements AirGatewayFilterManager {

    @Resource
    private ApplicationContext applicationContext;

    private final Map<AirGatewayStrategy, List<AbstractAirGatewayFilter>> customerFiltersChainMap = new ConcurrentHashMap<>();
    private final Map<String, AbstractAirGatewayFilter> customerAirGatewayFilterMap = new ConcurrentHashMap<>();
    private final Map<String, AirGatewayStrategy> customerFilterStrategyMap = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    @PostConstruct
    public void init() {
        //把所有的AbstractAirGatewayFilter注入进来
        customerAirGatewayFilterMap.putAll(applicationContext.getBeansOfType(AbstractAirGatewayFilter.class));
        //把所有的AirGatewayStrategy注入进来
        customerFilterStrategyMap.putAll(applicationContext.getBeansOfType(AirGatewayStrategy.class));
        //调用一次buildAllFilterChainByStrategy(),初始化customerFiltersChainMap
        buildAllFilterChainByStrategy(customerFilterStrategyMap);
    }

    @Override
    public void buildAllFilterChainByStrategy(Map<String, AirGatewayStrategy> customerFilterStrategyMap) {
        writeLock.lock();
        //todo 刷线filterChain的编排逻辑
        try {
            Map<AirGatewayStrategy, List<AbstractAirGatewayFilter>> newChainMap = new ConcurrentHashMap<>();
            customerFilterStrategyMap.forEach((beanName, strategy) -> {
                List<AbstractAirGatewayFilter> filterChain = buildFilterChainForStrategy(strategy);
                newChainMap.put(strategy, filterChain);
            });
            // 原子性替换整个映射
            customerFiltersChainMap.clear();
            customerFiltersChainMap.putAll(newChainMap);

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Mono<Void> executeFilterChain(@NonNull AirGatewayStrategy strategy,@NonNull ServerWebExchange exchange, WebFilterChain originalChain) {
        readLock.lock();
        if (ObjectUtils.isEmpty(originalChain)) {
            originalChain = new DefaultWebFilterChain(webHandler -> {
                // 返回空的 Mono，表示处理完成但不做任何操作
                return Mono.empty();
            }, Collections.emptyList());
        }
        try {
            List<AbstractAirGatewayFilter> filterChain = customerFiltersChainMap.get(strategy);
            List<AbstractAirGatewayFilter> enabledFilters = filterChain.stream()
                    .filter(filter -> !filter.isDisable())
                    .collect(Collectors.toList());
            // 执行自定义的过滤器链 -- 关键：每次请求创建新实例
            CustomWebFilterChain customChain = new CustomWebFilterChain(filterChain, originalChain);
            return customChain.filter(exchange);
        }finally {
            readLock.unlock();
        }
    }

    private List<AbstractAirGatewayFilter> buildFilterChainForStrategy(AirGatewayStrategy strategy) {

        List<AbstractAirGatewayFilter> filterChain = new CopyOnWriteArrayList<>();
        List<String> filterKeys = strategy.getStrategyOrder()
                .subList(0, strategy.getStrategyOrder().size());

        filterKeys.stream()
                .map(customerAirGatewayFilterMap::get)
                .filter(Objects::nonNull) // 过滤掉null值
                .forEach(filterChain::add);
        log.info("策略 {} 构建filterChain成功，包含 {} 个过滤器",
                strategy.getClass().getSimpleName(), filterChain.size());
      return filterChain;
    }

    public AirGatewayStrategy findAirGatewayStrategyName(String strategyName) {
        return customerFiltersChainMap.keySet().stream()
                .filter(strategy -> strategyName.equals(strategy.getStrategyName()))
                .findFirst()
                .orElse(null);
    }
}
