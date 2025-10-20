package com.airfree.cache.config.redis;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@NoArgsConstructor
@ConfigurationProperties(prefix = "redisson")
public class AirRedissonConfigProperties {

    private String singleConfig;
    private String clusterConfig;
    private String mode;

}
