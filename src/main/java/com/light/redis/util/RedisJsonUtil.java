package com.light.redis.util;

import com.light.config.util.ApplicationContextUtil;
import com.light.redis.config.CustomLettuceConnectionConfiguration;
import com.light.redis.model.RedisInfo;
import io.lettuce.core.resource.ClientResources;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RedisJsonUtil {
    private static final ConcurrentHashMap<Integer, RedisTemplate> REDIS_TEMPLATE__MAP = new ConcurrentHashMap<>();


    public static <K, V> RedisTemplate<K, V> getRedisTemplate(int dbIndex) {
        RedisTemplate<K, V> redisTemplate = REDIS_TEMPLATE__MAP.get(dbIndex);
        if (redisTemplate != null) {
            return redisTemplate;
        }
        RedisConnectionFactory redisConnectionFactory = RedisConnectionFactoryUtil.getRedisConnectionFactory(dbIndex);
        redisTemplate = RedisTemplateUtil.getRedisTemplate(redisConnectionFactory);
        REDIS_TEMPLATE__MAP.put(dbIndex, redisTemplate);
        return redisTemplate;
    }


    public static <K, V> RedisTemplate<K, V> getRedisTemplate() {
        return ApplicationContextUtil.getBean("jsonRedisTemplate", RedisTemplate.class);
    }


    public static <V> V get(String key, int dbIndex) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.get(key);
    }

    public static <V> V get(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.get(key);
    }

    public static <K, V> List<V> multiGet(Collection<K> keys, int dbIndex) {
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        return valueOperations.multiGet(keys);
    }

    public static <K, V> void multiSet(Map<? extends K, ? extends V> map, int dbIndex) {
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        valueOperations.multiSet(map);
    }

    public static <K, V> Boolean multiSetIfAbsent(Map<? extends K, ? extends V> map, int dbIndex) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        return valueOperations.multiSetIfAbsent(map);
    }

    public static <K, V> List<V> multiGet(Collection<K> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate();
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        return valueOperations.multiGet(keys);
    }


    public static <K, V> void multiSet(Map<? extends K, ? extends V> map) {
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate();
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        valueOperations.multiSet(map);
    }

    public static <K, V> Boolean multiSetIfAbsent(Map<? extends K, ? extends V> map) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        RedisTemplate<K, V> redisTemplate = getRedisTemplate();
        ValueOperations<K, V> valueOperations = redisTemplate.opsForValue();
        return valueOperations.multiSetIfAbsent(map);
    }


    public static <V> boolean set(String key, V value, int dbIndex, long expire) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static <V> boolean set(String key, V value, long expire) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static <V> boolean setIfAbsent(String key, V value, int dbIndex) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.setIfAbsent(key, value);
    }

    public static <V> boolean setIfAbsent(String key, V value) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.setIfAbsent(key, value);
    }




}

