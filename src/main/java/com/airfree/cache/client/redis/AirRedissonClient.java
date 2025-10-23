package com.airfree.cache.client.redis;

import com.airfree.cache.config.redis.AirRedissonConfigFactory;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.config.Config;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AirRedissonClient {

    private final AirRedissonConfigFactory airRedissonConfigFactory;
    private final RedissonReactiveClient redissonReactiveClient;

    public AirRedissonClient(AirRedissonConfigFactory configFactory) {
        this.airRedissonConfigFactory = configFactory;
        log.info("开始创建redissonReactiveClient....");
        this.redissonReactiveClient = initRedissonReactiveClient();
        log.info("创建redissonReactiveClient成功: {}", this.redissonReactiveClient);
    }

    private RedissonReactiveClient initRedissonReactiveClient() {
        Config config = this.airRedissonConfigFactory.createConfig();
        return Redisson.create(config).reactive();
    }

    @PreDestroy
    public void destroy() {
        log.info("正在销毁所有 RedissonReactiveClient...");
        this.redissonReactiveClient.shutdown();
    }

}
