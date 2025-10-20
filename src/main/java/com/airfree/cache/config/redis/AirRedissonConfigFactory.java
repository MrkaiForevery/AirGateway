package com.airfree.cache.config.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class AirRedissonConfigFactory {

    private final AirRedissonConfigProperties redissonConfigProperties;

    public AirRedissonConfigFactory(AirRedissonConfigProperties properties){
        this.redissonConfigProperties = properties;
    }

    public org.redisson.config.Config createConfig() {
        String configContent = null;
        if ("single".equals(redissonConfigProperties.getMode())) {
            configContent = redissonConfigProperties.getSingleConfig();
        }else if ("cluster".equals(redissonConfigProperties.getMode())) {
            configContent = redissonConfigProperties.getClusterConfig();
        }else{
            log.info("不支持的redisson配置模式，请检查配置！！！");
            throw new RuntimeException("不支持的redisson配置模式，请检查配置！！！");
        }

        log.info("开始解析redisson的配置...");
        org.redisson.config.Config config = null;
        try {
            config = org.redisson.config.Config.fromYAML(configContent);
            log.info("Redisson配置解析成功");
        }catch (IOException e){
            log.error("解析Redisson配置失败", e);
            throw new RuntimeException("Redisson配置解析失败", e);
        }
        return config;
    }

}
