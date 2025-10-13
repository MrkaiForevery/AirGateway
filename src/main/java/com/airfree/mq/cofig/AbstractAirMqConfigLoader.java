package com.airfree.mq.cofig;

public abstract class AbstractAirMqConfigLoader {

    public abstract void staticLoad();

    public abstract void dynamicLoad();

    public abstract String getLoadMode();

    public abstract void setLoadMode(String loadMode);

    public abstract String getLoaderName();

    public abstract Class<? extends AbstractAirMqConfig> supportMqConfigType();
}
