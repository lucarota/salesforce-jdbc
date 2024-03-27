package com.ascendix.jdbc.salesforce.cache;

import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import java.sql.ResultSetMetaData;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

public class CacheConfig {
    private final CacheManager cacheManager;

    private static final CacheConfig INSTANCE;
    static {
        INSTANCE = new CacheConfig();
    }

    public static CacheConfig getInstance() {
        return INSTANCE;
    }

    private CacheConfig() {
        cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build();
        cacheManager.init();

        cacheManager.createCache("DataCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, CachedResultSet.class,
                        ResourcePoolsBuilder.heap(100)));

        cacheManager.createCache("MetadataCache",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, ResultSetMetaData.class,
                        ResourcePoolsBuilder.heap(100)));
    }

    public Cache<String, CachedResultSet> getDataCache() {
        return cacheManager.getCache("DataCache", String.class, CachedResultSet.class);
    }

    public Cache<String, ResultSetMetaData> getMetaDataCache() {
        return cacheManager.getCache("MetadataCache", String.class, ResultSetMetaData.class);
    }

}
