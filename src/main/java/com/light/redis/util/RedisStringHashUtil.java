package com.light.redis.util;

import org.springframework.data.redis.core.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisStringHashUtil {

    public static  Long delete(int dbIndex, String key, Object... hasStringeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.delete(key, hasStringeys);
    }

    public static  Boolean hasKey(String key, Object hasStringey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.hasKey(key, hasStringey);
    }


    public static  String get(String key, Object hasStringey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.get(key, hasStringey);
    }

    public static  List<String> multiGet(String key, Collection<String> hasStringeys, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.multiGet(key, hasStringeys);
    }


    public static  Long increment(String key, String hasStringey, long delta, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hasStringey, delta);
    }

    public static  Double increment(String key, String hasStringey, double delta, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hasStringey, delta);
    }


    public static  Set<String> keys(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.keys(key);
    }

    public static  Long size(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.size(key);
    }


    public static  void putAll(String key, Map<? extends String, ? extends String> m, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        hashOperations.putAll(key, m);
    }


    public static  void put(String key, String hasStringey, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        hashOperations.put(key, hasStringey, value);
    }


    public static  Boolean putIfAbsent(String key, String hasStringey, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.putIfAbsent(key, hasStringey, value);
    }


    public static  List<String> values(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.values(key);
    }


    public static  Map<String, String> entries(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.entries(key);
    }


    public static  Cursor<Map.Entry<String, String>> scan(String key, ScanOptions options, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.scan(key, options);
    }




    public static  Long delete(String key, Object... hasStringeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.delete(key, hasStringeys);
    }

    public static  Boolean hasKey(String key, Object hasStringey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.hasKey(key, hasStringey);
    }


    public static  String get(String key, Object hasStringey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.get(key, hasStringey);
    }

    public static  List<String> multiGet(String key, Collection<String> hasStringeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.multiGet(key, hasStringeys);
    }


    public static  Long increment(String key, String hasStringey, long delta) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hasStringey, delta);
    }

    public static  Double increment(String key, String hasStringey, double delta) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.increment(key, hasStringey, delta);
    }


    public static  Set<String> keys(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.keys(key);
    }

    public static  Long size(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.size(key);
    }


    public static  void putAll(String key, Map<? extends String, ? extends String> m) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        hashOperations.putAll(key, m);
    }


    public static  void put(String key, String hasStringey, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        hashOperations.put(key, hasStringey, value);
    }


    public static  Boolean putIfAbsent(String key, String hasStringey, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.putIfAbsent(key, hasStringey, value);
    }


    public static  List<String> values(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.values(key);
    }


    public static  Map<String, String> entries(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.entries(key);
    }


    public static  Cursor<Map.Entry<String, String>> scan(String key, ScanOptions options) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        return hashOperations.scan(key, options);
    }
}
