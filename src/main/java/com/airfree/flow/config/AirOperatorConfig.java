package com.airfree.flow.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class AirOperatorConfig {

    private String operator;
    private String resource;
    private Map<String, Object> config = new HashMap<>();
    private int orderId;
}
