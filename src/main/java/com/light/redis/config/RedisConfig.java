package com.light.redis.config;

import com.light.redis.util.RedisTemplateUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
@ConditionalOnClass(RedisOperations.class)
@EnableConfigurationProperties(RedisProperties.class)
public class RedisConfig {


    @Bean("redisTemplate")
    public <K, V> RedisTemplate<K, V> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<K, V> redisTemplate = RedisTemplateUtil.getCustomRedisTemplateWithDefaultValueSerializer(redisConnectionFactory);
        return redisTemplate;
    }


    @Bean("jsonRedisTemplate")
    public <K, V> RedisTemplate<K, V> jsonRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<K, V> redisTemplate = RedisTemplateUtil.getRedisTemplate(redisConnectionFactory);
        return redisTemplate;
    }


    @Bean("stringRedisTemplate")
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate stringRedisTemplate = RedisTemplateUtil.getStringRedisTemplate(redisConnectionFactory);
        return stringRedisTemplate;
    }




}
