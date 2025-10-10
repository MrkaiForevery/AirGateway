package com.airfree.entity.request;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.Map;

@Data
public class AirGatewayInternalRequest {
    private String requestId;
    private String path;
    private HttpMethod method;
    private HttpHeaders headers;
    private MultiValueMap<String, String> queryParams;
    private byte[] body;
    private Map<String, Object> attributes;
    private String remoteAddress;
    private long startTime;
    private boolean supportedProtocol = false;

    public String getHeader(String name) {
        return headers.get(name);
    }

    public String getQueryParam(String name) {
        return queryParams.getFirst(name);
    }

    public void setAttribute(String key, Object value) {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        attributes.put(key, value);
    }

    public <T> T getAttribute(String key) {
        return attributes != null ? (T) attributes.get(key) : null;
    }


}
