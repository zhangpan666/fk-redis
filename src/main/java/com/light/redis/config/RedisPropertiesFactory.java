package com.light.redis.config;

import com.light.config.util.PropertiesUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisPropertiesFactory {
    private static final RedisProperties redisProperties = new RedisProperties();


    public static RedisProperties getRedisProperties() {
        return redisProperties;
    }

    @Data
    public static class RedisProperties {

        private String host;

        private int port;

        private String password;

        private int database;

        private int maxIdle;

        private int maxActive;

        private int minIdle;

        private int maxWait;

        private int timeout;

        private long timeEvictionRun;


        private RedisProperties() {
            host = PropertiesUtil.getProperty("spring.redis.host", String.class);
            port = PropertiesUtil.getProperty("spring.redis.port", int.class, 6379);
            password = PropertiesUtil.getProperty("spring.redis.password", String.class);
            database = PropertiesUtil.getProperty("spring.redis.database", int.class, 0);
            maxIdle = PropertiesUtil.getProperty("spring.redis.pool.max-idle", int.class, 8);
            minIdle = PropertiesUtil.getProperty("spring.redis.pool.min-idle", int.class, 1);
            maxActive = PropertiesUtil.getProperty("spring.redis.pool.max-active", int.class, 600);
            maxWait = PropertiesUtil.getProperty("spring.redis.pool.max-wait", int.class, 10000);
            timeout = PropertiesUtil.getProperty("spring.redis.timeout", int.class, 20000);
            timeEvictionRun = PropertiesUtil.getProperty("spring.redis.pool.time-between-eviction-runs", long.class, 300000L);
            log.info("redis初始化，host={}", host);
        }


    }


}
