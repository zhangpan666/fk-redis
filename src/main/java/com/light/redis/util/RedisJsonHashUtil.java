package com.light.redis.util;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisJsonHashUtil {

    public static <H, HK, HV> Long delete(int dbIndex, H key, Object... hashKeys) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.delete(key, hashKeys);
    }

    public static <H, HK, HV> Boolean hasKey(H key, Object hashKey, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.hasKey(key, hashKey);
    }


    public static <H, HK, HV> HV get(H key, Object hashKey, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.get(key, hashKey);
    }

    public static <H, HK, HV> List<HV> multiGet(H key, Collection<HK> hashKeys, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.multiGet(key, hashKeys);
    }


    public static <H, HK, HV> Long increment(H key, HK hashKey, long delta, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hashKey, delta);
    }

    public static <H, HK, HV> Double increment(H key, HK hashKey, double delta, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hashKey, delta);
    }


    public static <H, HK, HV> Set<HK> keys(H key, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.keys(key);
    }

    public static <H, HK, HV> Long size(H key, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.size(key);
    }


    public static <H, HK, HV> void putAll(H key, Map<? extends HK, ? extends HV> m, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        hashOperations.putAll(key, m);
    }


    public static <H, HK, HV> void put(H key, HK hashKey, HV value, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        hashOperations.put(key, hashKey, value);
    }


    public static <H, HK, HV> Boolean putIfAbsent(H key, HK hashKey, HV value, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.putIfAbsent(key, hashKey, value);
    }


    public static <H, HK, HV> List<HV> values(H key, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.values(key);
    }


    public static <H, HK, HV> Map<HK, HV> entries(H key, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.entries(key);
    }


    public static <H, HK, HV> Cursor<Map.Entry<HK, HV>> scan(H key, ScanOptions options, int dbIndex) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.scan(key, options);
    }




    public static <H, HK, HV> Long delete(H key, Object... hashKeys) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.delete(key, hashKeys);
    }

    public static <H, HK, HV> Boolean hasKey(H key, Object hashKey) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.hasKey(key, hashKey);
    }


    public static <H, HK, HV> HV get(H key, Object hashKey) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.get(key, hashKey);
    }

    public static <H, HK, HV> List<HV> multiGet(H key, Collection<HK> hashKeys) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.multiGet(key, hashKeys);
    }


    public static <H, HK, HV> Long increment(H key, HK hashKey, long delta) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hashKey, delta);
    }

    public static <H, HK, HV> Double increment(H key, HK hashKey, double delta) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hashKey, delta);
    }


    public static <H, HK, HV> Set<HK> keys(H key) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.keys(key);
    }

    public static <H, HK, HV> Long size(H key) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.size(key);
    }


    public static <H, HK, HV> void putAll(H key, Map<? extends HK, ? extends HV> m) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        hashOperations.putAll(key, m);
    }


    public static <H, HK, HV> void put(H key, HK hashKey, HV value) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        hashOperations.put(key, hashKey, value);
    }


    public static <H, HK, HV> Boolean putIfAbsent(H key, HK hashKey, HV value) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.putIfAbsent(key, hashKey, value);
    }


    public static <H, HK, HV> List<HV> values(H key) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.values(key);
    }


    public static <H, HK, HV> Map<HK, HV> entries(H key) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.entries(key);
    }


    public static <H, HK, HV> Cursor<Map.Entry<HK, HV>> scan(H key, ScanOptions options) {
        RedisTemplate<H, HV> redisTemplate = RedisJsonUtil.getRedisTemplate();
        HashOperations<H, HK, HV> hashOperations = redisTemplate.opsForHash();
        return hashOperations.scan(key, options);
    }

}
