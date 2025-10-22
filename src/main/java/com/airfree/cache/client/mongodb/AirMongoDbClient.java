package com.airfree.cache.client.mongodb;

import com.airfree.cache.config.mongo.AirMongoDbConfigProperties;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class AirMongoDbClient {

    private final AirMongoDbConfigProperties properties;
    private final MongoClient mongoClient;
    private final ReactiveMongoTemplate mongoTemplate;

    public AirMongoDbClient(AirMongoDbConfigProperties properties) {
        this.properties = properties;
        log.info("开始创建mongodb的连接client......");
        this.mongoClient = initReactiveMongoDbClient();
        this.mongoTemplate = initReactiveMongoTemplate();
        log.info("mongodb连接创建成功,client {}, template {}", this.mongoClient, this.mongoTemplate);
    }

    private ReactiveMongoTemplate initReactiveMongoTemplate() {
        return new ReactiveMongoTemplate(this.mongoClient, properties.getDatabase());
    }

    private MongoClient initReactiveMongoDbClient() {
        ConnectionString connectionString = buildConnectionString();
        // todo 使用 MongoClientSettings.Builder 进行详细配置
        MongoClientSettings mongoClientSettings = buildClientSettings(connectionString);
        return MongoClients.create(mongoClientSettings);
    }

    private ConnectionString buildConnectionString() {
        //设置连接字符串
        ConnectionString connectionString = new ConnectionString(
                "mongodb://" + this.properties.getUsername() + ":" +
                        this.properties.getPassword() + "@" +
                        this.properties.getHost() + ":" +
                        this.properties.getPort() + "/" +
                        this.properties.getDatabase() +
                        "?authSource=" + this.properties.getAuthenticationDatabase());
        return connectionString;
    }

    private MongoClientSettings buildClientSettings(ConnectionString connectionString) {
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
        settingsBuilder.applicationName(this.properties.getApplicationName());
        settingsBuilder.applyConnectionString(connectionString);

        // 配置连接池设置
        Map<String, Object> poolConfig = this.properties.getPoolConfig();
        settingsBuilder.applyToConnectionPoolSettings(builder -> builder
                .maxSize(Integer.parseInt(poolConfig.get("maxSize").toString())) // 连接池最大连接数
                .minSize(Integer.parseInt(poolConfig.get("minSize").toString()))  // 连接池最小连接数
                .maxWaitTime(Long.parseLong(poolConfig.get("maxWaitTime").toString()), TimeUnit.MILLISECONDS) // 最大等待获取连接时间(毫秒)
                .maxConnectionLifeTime(Long.parseLong(poolConfig.get("maxConnectionLifeTime").toString()), TimeUnit.MILLISECONDS) // 连接的最大生存时间(毫秒)

        );
        // 配置Socket设置
        Map<String, Object> socketConfig = this.properties.getSocketConfig();
        settingsBuilder.applyToSocketSettings(builder -> builder
                .connectTimeout(Integer.parseInt(socketConfig.get("connectTimeout").toString()), TimeUnit.MILLISECONDS) // 连接超时时间(毫秒)
                .readTimeout(Integer.parseInt(socketConfig.get("readTimeout").toString()), TimeUnit.MILLISECONDS)    // 读取超时时间(毫秒)
        );
        // 配置服务器设置
        Map<String, Object> serverConfig = this.properties.getServerConfig();
        settingsBuilder.applyToServerSettings(builder -> builder
                .heartbeatFrequency(Long.parseLong(serverConfig.get("heartbeatFrequency").toString()), TimeUnit.MILLISECONDS) // 心跳频率(毫秒)
                .minHeartbeatFrequency(Long.parseLong(serverConfig.get("minHeartbeatFrequency").toString()), TimeUnit.MILLISECONDS) // 最小心跳间隔(毫秒))
        );

        //ssl配置
        Map<String, Object> sslConfig = this.properties.getSslConfig();
        settingsBuilder.applyToSslSettings(builder ->
                builder.enabled(Boolean.valueOf(sslConfig.get("enable").toString())) // 根据实际情况启用或禁用SSL)
        );

        // 配置压缩设置
        settingsBuilder.compressorList(Arrays.asList(
                // 可以配置多个压缩器，驱动程序会按顺序协商
                MongoCompressor.createSnappyCompressor(),
                MongoCompressor.createZlibCompressor()
        ));

        return settingsBuilder.build();
    }
}
