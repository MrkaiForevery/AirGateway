package com.airfree.discovery.event;

import com.airfree.discovery.instance.AirServiceInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AirServiceInstanceEvent {

    private String serviceId;
    private AirServiceInstance instance;
    private EventType eventType; // ADDED, REMOVED, MODIFIED

    public enum EventType {
        ADDED, REMOVED, MODIFIED
    }

}
