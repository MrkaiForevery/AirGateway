package com.airfree.flow.core;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirFlowControlConfigProperties;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.enums.AirFlowControlType;
import com.airfree.flow.operator.circuitBreaker.AirGeneralCircuitBreakerOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 流控核心类
 * 流控建造器，用于构建一个完整的流控操作流程，可以从外部读取数据进行配置*
 */
@Slf4j
@Component
public class AirFlowControlPipelineBuilder {

    private final AirFlowControlConfigProperties controlConfigProperties;

    public AirFlowControlPipelineBuilder(AirFlowControlConfigProperties controlConfigProperties) {
        this.controlConfigProperties = controlConfigProperties;
    }

    public <T> Function<Mono<T>, Mono<T>> buildPipeline(String pipelineName, String[] currentResources) {
        final List<AirOperatorConfig> airOperatorConfigList = controlConfigProperties.getPipelines().get(pipelineName);
        if (airOperatorConfigList.size() <= 0) {
            return mono -> mono;
        }
        //排序
        List<AirOperatorConfig> airOperatorConfigs = sortOperatorChain(airOperatorConfigList);
        return mono -> {
            Mono<T> result = mono;
            //todo这里要进行排序操作，把多个operator执行的顺序排好
            for (int i = 0; i < airOperatorConfigs.size(); i++) {
                //todo  这里需要根据operatorConfig中的配置来初始化operator实例，因为后期需要实现配置热更新所以，所以这里只能每次重新构建
                String operatorName = airOperatorConfigs.get(i).getOperator();
                //排除不支持的operator类型
                if (!AirFlowControlType.contains(operatorName) || operatorName == null) {
                    log.info("operatorName不合法无法进行下一步创建工作！！！");
                    throw new RuntimeException("operatorName不合法无法进行下一步创建工作！！！");
                }
                //todo 这里执行构建operator操作，对于CircuitBreakerOperator需要传入一个fallbackFunction降级函数，这里先传一个空的
                Function<Throwable, Mono<T>> fallbackFunction = null;
                AirFlowControlOperator<T> operator = buildOperatorByReflectionWay(operatorName, currentResources, airOperatorConfigs.get(i), fallbackFunction);
                result = result.transform(operator);
            }
            log.info("流控执行链完全通过！！！");
            return result;
        };
    }

    private <T> AirFlowControlOperator<T> buildOperatorByReflectionWay(String operatorName, String[] currentResources, AirOperatorConfig config, Function<Throwable, Mono<T>> fallbackFunction) {
        //TODO 这里使用AirGeneralCircuitBreakerOperator作为熔断装置

        //判断是否是系统支持的流控操作器
        AirFlowControlType byFlowTypeName = AirFlowControlType.getByFlowTypeName(config.getOperator());
        if (byFlowTypeName == null) {
            log.info("系统不支持的流控操作类型！！！");
            throw new RuntimeException("系统不支持的流控操作类型！！！");
        }

        //判断资源列表是否支持(权限控制)
        if (!this.isSupportResource(currentResources, config.getResource())) {
            log.info("该资源不存在于资源权限控制列表中，不允许构建相关的CircuitBreakerOperator，程序终止！！！");
            throw new RuntimeException("该资源不存在于资源权限控制列表中，不允许构建相关的CircuitBreakerOperator，程序终止！！！");
        }

        //如果是熔断操作,就创造一个熔断器
        if (AirFlowControlType.CIRCUIT_BREAKER.equals(operatorName)) {
            return new AirGeneralCircuitBreakerOperator(operatorName, config, fallbackFunction);
        }
        //TODO 这里还有限流装置和降级装置没有做.....

        return null;
    }

    private List<AirOperatorConfig> sortOperatorChain(List<AirOperatorConfig> configs) {
        //根据orderId字段进行从小到大排序
        return configs.stream()
                .sorted(Comparator.comparingInt(AirOperatorConfig::getOrderId))
                .collect(Collectors.toList());
    }

    private static boolean isSupportResource(String[] currentResources, String supportResources) {
        if (currentResources.length <= 0) {
            log.error("限定资源为空，表示无限制资源，不能进行这样不安全操作！！！");
            return false;
        }
        List<String> resourcesList = Arrays.stream(currentResources).collect(Collectors.toCollection(ArrayList::new));
        String[] supportResourcesArray = supportResources.split(";");
        ArrayList<String> supportResourcesList = Arrays.stream(supportResourcesArray).collect(Collectors.toCollection(ArrayList::new));
        for (String re : resourcesList) {
            if (supportResourcesList.contains(re)) {
                return true;
            }
        }
        return false;
    }
}
