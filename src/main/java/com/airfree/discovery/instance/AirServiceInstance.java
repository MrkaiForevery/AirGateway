package com.airfree.discovery.instance;

import lombok.Data;

import java.net.URI;
import java.util.Map;

@Data
public class AirServiceInstance {

    private String instanceId;
    private String serviceId;
    private String host;
    private int port;
    private boolean secure;
    private Map<String, String> metadata;
    private URI uri;

    public URI getUri() {
        if (uri == null) {
            String scheme = secure ? "https" : "http";
            uri = URI.create(String.format("%s://%s:%d", scheme, host, port));
        }
        return uri;
    }

}
