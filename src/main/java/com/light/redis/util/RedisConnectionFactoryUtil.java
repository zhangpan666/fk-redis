package com.light.redis.util;

import com.light.config.util.ApplicationContextUtil;
import com.light.redis.config.CustomLettuceConnectionConfiguration;
import io.lettuce.core.resource.ClientResources;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

public class RedisConnectionFactoryUtil {



    public static RedisConnectionFactory getRedisConnectionFactory(int dbIndex) {
        CustomLettuceConnectionConfiguration lettuceConnectionConfiguration = ApplicationContextUtil.getBean(CustomLettuceConnectionConfiguration.class);
        ClientResources clientResources = ApplicationContextUtil.getBean(ClientResources.class);
        LettuceConnectionFactory redisConnectionFactory = lettuceConnectionConfiguration.redisConnectionFactory(clientResources);
        redisConnectionFactory.setDatabase(dbIndex);
        redisConnectionFactory.afterPropertiesSet();
        return redisConnectionFactory;
    }


    public static RedisConnectionFactory getRedisConnectionFactory(int dbIndex,CustomLettuceConnectionConfiguration
            customLettuceConnectionConfiguration,ClientResources clientResources) {
        LettuceConnectionFactory redisConnectionFactory = customLettuceConnectionConfiguration.redisConnectionFactory(clientResources);
        redisConnectionFactory.setDatabase(dbIndex);
        redisConnectionFactory.afterPropertiesSet();
        return redisConnectionFactory;
    }



}
