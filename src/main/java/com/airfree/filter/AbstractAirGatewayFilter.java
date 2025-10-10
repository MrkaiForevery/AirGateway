package com.airfree.filter;

import org.springframework.web.server.WebFilter;

import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractAirGatewayFilter implements WebFilter {

    public static final AtomicBoolean enabled  = new AtomicBoolean(false);

    public abstract void enable();

    public abstract void disable();

    public boolean isDisable(){
      return enabled.get();
    }
}
