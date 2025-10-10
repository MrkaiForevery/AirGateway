package com.airfree.filter.filterStrategy;

import com.airfree.filter.AirGatewayStrategy;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ApiFilterStrategy implements AirGatewayStrategy {

    private final List<String>  filterOrderList = new CopyOnWriteArrayList<>();

    private  Object cacheObject = null;


    @PostConstruct
    public void init(){
        flushStrategyOrder(cacheObject);
        //todo 这里不做处理，先手动set一个进去
        filterOrderList.add("apiInternalRequestConvertFilter");
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void flushStrategyOrder(Object cacheObject) {
        //todo 这里要有刷新flush的逻辑

    }

    @Override
    public List<String> getStrategyOrder() {
        return this.filterOrderList ;
    }
}
