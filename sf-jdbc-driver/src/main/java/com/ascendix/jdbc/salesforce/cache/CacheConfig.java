package com.ascendix.jdbc.salesforce.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class CacheConfig {
    private final CacheManager cacheManager;

    public CacheConfig() {
        cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build();
        cacheManager.init();

        cacheManager.createCache("DataCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, ResultSet.class,
                        ResourcePoolsBuilder.heap(100)));

        cacheManager.createCache("MetadataCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, ResultSetMetaData.class,
                        ResourcePoolsBuilder.heap(100)));
    }

    public Cache<String, ResultSet> getDataCache() {
        return cacheManager.getCache("DataCache", String.class, ResultSet.class);
    }

    public Cache<String, ResultSetMetaData> getMetaDataCache() {
        return cacheManager.getCache("MetadataCache", String.class, ResultSetMetaData.class);
    }

}
