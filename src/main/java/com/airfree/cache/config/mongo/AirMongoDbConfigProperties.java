package com.airfree.cache.config.mongo;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@NoArgsConstructor
@ConfigurationProperties(prefix = "mongodb")
public class AirMongoDbConfigProperties {

    private String host;
    private String port;
    private String authenticationDatabase;
    private String username;
    private String password;
    private String database;

}
