package com.airfree.cache.entity;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class AirCacheEntity {

    private String cacheId;
    private String baseOnDatabaseType; // REDIS, MONGODB, CASSANDRA
    private String key;
    private Object value;
    private String cacheDataType;   // STRING, HASH, LIST, SET, ZSET, DOCUMENT
    private String cacheBizType;
}
