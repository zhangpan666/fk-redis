package com.light.redis.util;

import com.light.config.util.ApplicationContextUtil;
import com.light.redis.model.RedisInfo;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RedisUtil {
    private static final ConcurrentHashMap<Integer, RedisTemplate> REDIS_TEMPLATE__MAP = new ConcurrentHashMap<>();


    public static <K, V> RedisTemplate<K, V> getRedisTemplate(int dbIndex) {
        RedisTemplate<K, V> redisTemplate = REDIS_TEMPLATE__MAP.get(dbIndex);
        if (redisTemplate != null) {
            return redisTemplate;
        }
        RedisConnectionFactory redisConnectionFactory = RedisConnectionFactoryUtil.getRedisConnectionFactory(dbIndex);
        redisTemplate = RedisTemplateUtil.getCustomRedisTemplateWithDefaultValueSerializer(redisConnectionFactory);
        REDIS_TEMPLATE__MAP.put(dbIndex, redisTemplate);
        return redisTemplate;
    }



    public static <K, V> RedisTemplate<K, V> getRedisTemplate() {
        return ApplicationContextUtil.getBean("redisTemplate", RedisTemplate.class);
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

    public static <V> boolean set(String key, V value, int dbIndex) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value);
        return true;
    }

    public static <V> boolean set(String key, V value) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value);
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

    public static <V> Long increment(String key, int dbIndex) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key);
    }

    public static <V> Long increment(String key,long delta, int dbIndex) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key,delta);
    }

    public static <V> Double increment(String key, double delta, int dbIndex) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key,delta);
    }

    public static <V> Long decrement(String key, int dbIndex) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.decrement(key);
    }

    public static <V> Long decrement(String key,long delta, int dbIndex) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.decrement(key,delta);
    }

    public static <V> Long increment(String key) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key);
    }

    public static <V> Long increment(String key,long delta) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key,delta);
    }

    public static <V> Double increment(String key, double delta) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.increment(key,delta);
    }

    public static <V> Long decrement(String key) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.decrement(key);
    }

    public static <V> Long decrement(String key,long delta) {
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.decrement(key,delta);
    }


    public static boolean delete(String key, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        redisTemplate.delete(key);
        return true;
    }

    public static boolean delete(String key) {
        RedisTemplate redisTemplate = getRedisTemplate();
        redisTemplate.delete(key);
        return true;
    }

    public static boolean deleteByPattern(String pattern, int dbIndex) {
        if (StringUtils.isEmpty(pattern)) {
            return false;
        }
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        Set keys = redisTemplate.keys(pattern + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return false;
        }
        redisTemplate.delete(keys);
        return true;
    }

    public static boolean deleteByPattern(String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            return false;
        }
        RedisTemplate redisTemplate = getRedisTemplate();
        Set keys = redisTemplate.keys(pattern + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return false;
        }
        redisTemplate.delete(keys);
        return true;
    }

    public static boolean deleteByPattern(RedisInfo redisInfo, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        Set keys = redisTemplate.keys(redisInfo.getKeyPrefix() + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return false;
        }
        redisTemplate.delete(keys);
        return true;
    }

    public static boolean deleteByPattern(RedisInfo redisInfo) {
        RedisTemplate redisTemplate = getRedisTemplate();
        Set keys = redisTemplate.keys(redisInfo.getKeyPrefix() + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return false;
        }
        redisTemplate.delete(keys);
        return true;
    }

    public static <K> Set<K> keys(String pattern, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.keys(pattern + "*");
    }

    public static <K> Set<K> keys(String pattern) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.keys(pattern + "*");
    }

    public static boolean deleteByKeys(Collection<String> keys, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        redisTemplate.delete(keys);
        return true;
    }

    public static boolean deleteByKeys(Collection keys) {
        RedisTemplate redisTemplate = getRedisTemplate();
        redisTemplate.delete(keys);
        return true;
    }

    public static boolean expire(String key, final long timeout, final TimeUnit unit, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.expire(key, timeout, unit);

    }

    public static boolean expire(String key, final long timeout, final TimeUnit unit) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.expire(key, timeout, unit);

    }

    public static boolean hasKey(String key, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.hasKey(key);
    }

    public static boolean hasKey(String key) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.hasKey(key);
    }

    public static boolean expireAt(String key, Date date, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.expireAt(key, date);
    }

    public static boolean expireAt(String key, Date date) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.expireAt(key, date);
    }

    public static boolean persist(String key, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.persist(key);
    }

    public static Boolean renameIfAbsent(String oldKey, String newKey, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.renameIfAbsent(oldKey, newKey);
    }

    public static Boolean renameIfAbsent(String oldKey, String newKey) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.renameIfAbsent(oldKey, newKey);
    }

    public static Long getExpire(String key, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.getExpire(key);
    }

    public static Long getExpire(String key) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.getExpire(key);
    }

    public static <K> K randomKey() {
        RedisTemplate<K, Object> redisTemplate = getRedisTemplate();
        return redisTemplate.randomKey();
    }

    public static <K> K randomKey(int dbIndex) {
        RedisTemplate<K, Object> redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.randomKey();
    }

    public static DataType type(String key, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.type(key);
    }

    public static DataType type(String key) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.type(key);
    }

    public static Long getExpire(String key, final TimeUnit timeUnit, int dbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.getExpire(key, timeUnit);
    }

    public static Long getExpire(String key, final TimeUnit timeUnit) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.getExpire(key, timeUnit);
    }

    public static Boolean move(String key, final int dbIndex, final int newDbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate(dbIndex);
        return redisTemplate.move(key, newDbIndex);
    }

    public static Boolean move(String key, final int newDbIndex) {
        RedisTemplate redisTemplate = getRedisTemplate();
        return redisTemplate.move(key, newDbIndex);
    }


}

