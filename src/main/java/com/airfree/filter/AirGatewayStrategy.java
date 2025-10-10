package com.airfree.filter;

import java.util.List;

public interface AirGatewayStrategy {

    String getStrategyName();

    void flushStrategyOrder(Object cacheObject);

    List<String> getStrategyOrder();

}
