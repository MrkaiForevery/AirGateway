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
        log.info("mongodb连接创建成功,client {}, template {}",this.mongoClient,this.mongoTemplate);
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
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                // 设置应用名称
                .applicationName("AirGateway")
                // 配置连接池设置
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(50) // 连接池最大连接数
                        .minSize(5)  // 连接池最小连接数
                        .maxWaitTime(120000, TimeUnit.MILLISECONDS) // 最大等待获取连接时间(毫秒)
                        .maxConnectionLifeTime(600000, TimeUnit.MILLISECONDS) // 连接的最大生存时间(毫秒)
                )
                // 配置Socket设置
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(10000, TimeUnit.MILLISECONDS) // 连接超时时间(毫秒)
                        .readTimeout(60000, TimeUnit.MILLISECONDS)    // 读取超时时间(毫秒)
                )
                // 配置服务器设置
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(10000, TimeUnit.MILLISECONDS) // 心跳频率(毫秒)
                        .minHeartbeatFrequency(500, TimeUnit.MILLISECONDS) // 最小心跳间隔(毫秒)
                )
                // 配置SSL设置 (如需启用SSL)
                .applyToSslSettings(builder ->
                        builder.enabled(false) // 根据实际情况启用或禁用SSL
                )
                // 配置压缩设置
                .compressorList(Arrays.asList(
                        // 可以配置多个压缩器，驱动程序会按顺序协商
                        MongoCompressor.createSnappyCompressor(),
                        MongoCompressor.createZlibCompressor()
                ))
                .build();
        return settings;
    }
}
