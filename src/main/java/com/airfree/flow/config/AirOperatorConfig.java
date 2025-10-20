package com.airfree.flow.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class AirOperatorConfig {

    private String operator;
    private String resource;
    private Map<String, Object> config = new HashMap<>();
    private int orderId;
}
