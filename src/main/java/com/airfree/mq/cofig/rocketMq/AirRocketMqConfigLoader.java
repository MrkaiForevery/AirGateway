package com.airfree.mq.cofig.rocketMq;

import com.airfree.mq.cofig.AbstractAirMqConfig;
import com.airfree.mq.cofig.AbstractAirMqConfigLoader;
import org.springframework.stereotype.Component;

@Component
public class AirRocketMqConfigLoader extends AbstractAirMqConfigLoader{

    private String loadMode = "static";

    public AirRocketMqConfigLoader(){
    }

    @Override
    public void staticLoad() {

    }

    @Override
    public void dynamicLoad() {

    }

    @Override
    public String getLoadMode() {
        return this.loadMode;
    }

    @Override
    public void setLoadMode(String loadMode) {
        this.loadMode = loadMode;
    }

    @Override
    public String getLoaderName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public Class<? extends AbstractAirMqConfig> supportMqConfigType() {
        return  AirRocketMqConfig.class;
    }
}
