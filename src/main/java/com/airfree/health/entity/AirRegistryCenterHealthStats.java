package com.airfree.health.entity;

import lombok.Getter;

import java.util.List;

@Getter
public class AirRegistryCenterHealthStats {

    private final int totalRegistries;
    private final long healthyRegistries;
    private final double healthRatio;
    private final List<AirRegistryCenterHealth> healthDetails;
    private final long checkTime;

    public AirRegistryCenterHealthStats(int totalRegistries, long healthyRegistries,
                               double healthRatio, List<AirRegistryCenterHealth> healthDetails) {
        this.totalRegistries = totalRegistries;
        this.healthyRegistries = healthyRegistries;
        this.healthRatio = healthRatio;
        this.healthDetails = healthDetails;
        this.checkTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("RegistryHealthStats{total=%d, healthy=%d, ratio=%.2f, checkTime=%d}",
                totalRegistries, healthyRegistries, healthRatio, checkTime);
    }
}
