package com.airfree.flow.core;

import com.airfree.flow.AirFlowControlOperator;
import com.airfree.flow.config.AirFlowControlConfigProperties;
import com.airfree.flow.config.AirOperatorConfig;
import com.airfree.flow.config.AirPipelineConfig;
import com.airfree.flow.enums.AirFlowControlType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 流控配置，用于构建一个完整的流控操作流程，可以从外部读取数据进行配置*
 */
@Slf4j
@Component
public class AirFlowControlPipelineBuilder {

    private final AirFlowControlConfigProperties controlConfigProperties;

    public AirFlowControlPipelineBuilder(AirFlowControlConfigProperties controlConfigProperties) {
        this.controlConfigProperties = controlConfigProperties;
    }

    public <T> Function<Mono<T>, Mono<T>> buildPipeline(String pipelineName) {
        AirPipelineConfig airPipelineConfig = controlConfigProperties.getPipelines().get(pipelineName);
        if (airPipelineConfig == null) {
            return mono -> mono;
        }
        return mono ->{
            Mono<T> result = mono;
            //todo这里要进行排序操作，把多个operator执行的顺序排好
            List<AirOperatorConfig> airOperatorConfigs = sortOperatorChain(airPipelineConfig.getOperators());
            for (int i = 0; i < airOperatorConfigs.size(); i++) {
                //todo  这里需要根据operatorConfig中的配置来初始化operator实例，因为后期需要实现配置热更新所以，所以这里只能每次重新构建
                String operatorName = airOperatorConfigs.get(i).getOperator();
                //排除不支持的operator类型
                if (!AirFlowControlType.contains(operatorName) || operatorName == null ) {
                    log.info("operatorName不合法无法进行下一步创建工作！！！");
                    throw new RuntimeException("operatorName不合法无法进行下一步创建工作！！！");
                }
                //todo 这里执行构建operator操作，可以考虑用反射的方式执行
                AirFlowControlOperator<T> operator = buildOperatorByReflectionWay();
                result = result.transform(operator);
            }
            return result;
        };
    }

    private <T> AirFlowControlOperator<T> buildOperatorByReflectionWay(){
        //TODO 未完待搞...，难点...

    }

    private List<AirOperatorConfig> sortOperatorChain(List<AirOperatorConfig> configs) {
        //根据orderId字段进行从小到大排序
        return configs.stream()
                .sorted(Comparator.comparingInt(AirOperatorConfig::getOrderId))
                .collect(Collectors.toList());
    }
}
