package com.light.redis.util;

import com.light.config.util.ApplicationContextUtil;
import com.light.redis.config.CustomLettuceConnectionConfiguration;
import com.light.redis.model.RedisInfo;
import io.lettuce.core.resource.ClientResources;
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

public class RedisStringUtil {
    private static final ConcurrentHashMap<Integer, StringRedisTemplate> STRING_REDIS_TEMPLATE_MAP = new ConcurrentHashMap<>();


    public static StringRedisTemplate getStringRedisTemplate(int dbIndex) {
        StringRedisTemplate stringRedisTemplate = STRING_REDIS_TEMPLATE_MAP.get(dbIndex);
        if (stringRedisTemplate != null) {
            return stringRedisTemplate;
        }
        RedisConnectionFactory redisConnectionFactory = RedisConnectionFactoryUtil.getRedisConnectionFactory(dbIndex);
        stringRedisTemplate = RedisTemplateUtil.getStringRedisTemplate(redisConnectionFactory);
        STRING_REDIS_TEMPLATE_MAP.put(dbIndex, stringRedisTemplate);
        return stringRedisTemplate;
    }



    public static StringRedisTemplate getStringRedisTemplate() {
        return ApplicationContextUtil.getBean("stringRedisTemplate", StringRedisTemplate.class);
    }



    public static String get(String key, int dbIndex) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.get(key);
    }

    public static String get(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.get(key);
    }

    public static boolean set(String key, String value, int dbIndex, long expire) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        stringStringValueOperations.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static boolean set(String key, String value, long expire) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        valueOperations.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static boolean setIfAbsent(String key, String value, int dbIndex) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        return valueOperations.setIfAbsent(key, value);
    }

    public static boolean setIfAbsent(String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate();
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.setIfAbsent(key, value);
    }

    public static List<String> multiGet(Collection<String> keys, int dbIndex) {
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.multiGet(keys);
    }

    public static void multiSet(Map<? extends String, ? extends String> map, int dbIndex) {
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        stringStringValueOperations.multiSet(map);
    }

    public static Boolean multiSetIfAbsent(Map<? extends String, ? extends String> map, int dbIndex) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.multiSetIfAbsent(map);
    }

    public static List<String> multiGet(Collection<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.multiGet(keys);
    }

    public static void multiSet(Map<? extends String, ? extends String> map) {
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        stringStringValueOperations.multiSet(map);
    }

    public static Boolean multiSetIfAbsent(Map<? extends String, ? extends String> map) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.multiSetIfAbsent(map);
    }


}

