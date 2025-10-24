package com.airfree.discovery.cache;

import com.airfree.discovery.instance.AirServiceInstance;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 使用外部Caffeine缓存当作discovery的instance暂存刷新
 */
@Slf4j
public class AirServiceDiscoveryCache {

    private final Cache<String, AirCacheEntry<List<AirServiceInstance>>> cache;

    private final long ttlSeconds;

    public AirServiceDiscoveryCache(long ttlSeconds, int maxSize) {
        this.ttlSeconds = ttlSeconds;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .maximumSize(maxSize)
                .recordStats()
                .build();
    }

    /**
     * 获取或加载服务实例
     */
    public Flux<AirServiceInstance> getOrLoad(String serviceId, Function<String, Flux<AirServiceInstance>> loader) {
        return Mono.fromCallable(() -> {
            AirCacheEntry<List<AirServiceInstance>> cached = cache.getIfPresent(serviceId);
            if (cached != null && !cached.isExpired()) {
                log.debug("缓存命中: {}", serviceId);
                return cached.getData();
            }
            return null;
        }).flatMapMany(cachedInstances -> {
            if (cachedInstances != null) {
                return Flux.fromIterable(cachedInstances);
            } else {
                log.debug("缓存未命中，从注册中心加载: {}", serviceId);
                return loader.apply(serviceId)
                        .collectList()
                        .doOnNext(instances -> {
                            // 缓存结果
                            AirCacheEntry<List<AirServiceInstance>> entry =
                                    new AirCacheEntry<>(instances, System.currentTimeMillis(), this.ttlSeconds);
                            cache.put(serviceId, entry);
                            log.debug("缓存服务实例: {}, 数量: {}", serviceId, instances.size());
                        })
                        .flatMapMany(Flux::fromIterable);
            }
        });
    }

    /**
     * 使缓存失效
     */
    // 添加缓存管理方法
    public Mono<Void> invalidateCache(String serviceId) {
        return Mono.fromRunnable(() -> cache.invalidate(serviceId));
    }

    public Mono<Void> invalidateAllCaches() {
        return Mono.fromRunnable(cache::invalidateAll);
    }

    /**
     * 获取缓存统计信息
     */
    public Map<String, Object> getStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
        Map<String, Object> statsMap = new HashMap<>();
        statsMap.put("hitCount", stats.hitCount());
        statsMap.put("missCount", stats.missCount());
        statsMap.put("loadSuccessCount", stats.loadSuccessCount());
        statsMap.put("loadFailureCount", stats.loadFailureCount());
        statsMap.put("totalLoadTime", stats.totalLoadTime());
        statsMap.put("evictionCount", stats.evictionCount());
        statsMap.put("estimatedSize", cache.estimatedSize());
        return statsMap;
    }


    @Data
    @AllArgsConstructor
    public static class AirCacheEntry<T> {

        private T data;
        private long timestamp;
        private long ttlSeconds;

        public boolean isExpired() {
            return System.currentTimeMillis() - timestamp > (ttlSeconds * 1000);
        }

    }

    public void clear(){
        cache.cleanUp();
    }

}
