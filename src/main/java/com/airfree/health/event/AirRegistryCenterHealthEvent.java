package com.airfree.health.event;

import com.airfree.health.entity.AirRegistryCenterHealth;

public class AirRegistryCenterHealthEvent {

    private final AirRegistryCenterHealth health;
    private final long eventTime;

    public AirRegistryCenterHealthEvent(AirRegistryCenterHealth health, long eventTime) {
        this.health = health;
        this.eventTime = eventTime;
    }

    public AirRegistryCenterHealth getHealth() { return health; }
    public long getEventTime() { return eventTime; }
}
