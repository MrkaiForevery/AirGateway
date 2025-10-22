package com.airfree.cache.config.mongo;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@ConfigurationProperties(prefix = "mongodb")
public class AirMongoDbConfigProperties {

    //连接参数
    private String applicationName;
    private String host;
    private String port;
    private String authenticationDatabase;
    private String username;
    private String password;
    private String database;
    //连接池参数
    private Map<String,Object> poolConfig = new HashMap<>();
    //socket参数
    private Map<String,Object> socketConfig = new HashMap<>();
    //服务器参数
    private Map<String,Object> serverConfig = new HashMap<>();
    //ssl
    private  Map<String,Object> sslConfig = new HashMap<>();



}
