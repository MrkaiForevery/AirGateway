package com.airfree.discovery.event;

import com.airfree.discovery.instance.AirServiceInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class AirServiceDiscoveryEvent {
    private String serviceId;
    private List<AirServiceInstance> instances;
}
