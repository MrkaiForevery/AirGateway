package com.airfree.rpc.entity;

import io.netty.handler.codec.http.HttpMethod;
import lombok.Data;

import java.util.Map;

@Data
public class AirRpcRequestContext {
    private String serviceName;
    private String methodPath;
    private HttpMethod httpMethod;
    private Map<String, String> headers;
    private String body;
    private Map<String, Object> attributes;
    private AirServiceEndpoint endpoint;
}
