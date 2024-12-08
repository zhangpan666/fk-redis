package com.light.redis.util;

import org.springframework.data.redis.core.*;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RedisStringSetUtil {

    public static  Long add(int dbIndex, String key, String... Stringalues) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.add(key, Stringalues);
    }

    public static  Long remove(int dbIndex, String key, String... Stringalues) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.remove(key, Stringalues);
    }

    public static  String pop(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.pop(key);
    }

    public static  Boolean move(String key, String Stringalue, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.move(key, Stringalue, destKey);
    }


    public static  Long size(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.size(key);
    }

    public static  Boolean isMember(String key, Object o, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.isMember(key, o);
    }

    public static  Set<String> intersect(String key, String otherKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKey);
    }


    public static  Set<String> intersect(String key, Collection<String> otherKeys, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKeys);
    }

    public static  Long intersectAndStore(String key, String otherKey, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static  Long intersectAndStore(String key, Collection<String> otherKeys, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static  Set<String> union(String key, String otherKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKey);
    }

    public static  Set<String> union(String key, Collection<String> otherKeys, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKeys);
    }


    public static  Long unionAndStore(String key, String otherKey, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKey, destKey);
    }


    public static  Long unionAndStore(String key, Collection<String> otherKeys, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static  Set<String> difference(String key, String otherKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKey);
    }

    public static  Set<String> difference(String key, Collection<String> otherKeys, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKeys);
    }


    public static  Long differenceAndStore(String key, String otherKey, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKey, destKey);
    }


    public static  Long differenceAndStore(String key, Collection<String> otherKeys, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKeys, destKey);
    }


    public static  Set<String> members(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.members(key);
    }


    public static  String randomMember(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMember(key);
    }


    public static  Set<String> distinctRandomMembers(String key, long count, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.distinctRandomMembers(key, count);
    }


    public static  List<String> randomMembers(String key, long count, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMembers(key, count);
    }

    public static  Cursor<String> scan(String key, ScanOptions options, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.scan(key, options);
    }

    public static  RedisOperations getOperations(int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.getOperations();
    }





    public static  Long add(String key, String... Stringalues) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.add(key, Stringalues);
    }

    public static  Long remove(String key, String... Stringalues) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.remove(key, Stringalues);
    }

    public static  String pop(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.pop(key);
    }

    public static  Boolean move(String key, String Stringalue, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.move(key, Stringalue, destKey);
    }


    public static  Long size(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.size(key);
    }

    public static  Boolean isMember(String key, Object o) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.isMember(key, o);
    }

    public static  Set<String> intersect(String key, String otherKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKey);
    }


    public static  Set<String> intersect(String key, Collection<String> otherKeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKeys);
    }

    public static  Long intersectAndStore(String key, String otherKey, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static  Long intersectAndStore(String key, Collection<String> otherKeys, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static  Set<String> union(String key, String otherKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKey);
    }

    public static  Set<String> union(String key, Collection<String> otherKeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKeys);
    }


    public static  Long unionAndStore(String key, String otherKey, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKey, destKey);
    }


    public static  Long unionAndStore(String key, Collection<String> otherKeys, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static  Set<String> difference(String key, String otherKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKey);
    }

    public static  Set<String> difference(String key, Collection<String> otherKeys) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKeys);
    }


    public static  Long differenceAndStore(String key, String otherKey, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKey, destKey);
    }


    public static  Long differenceAndStore(String key, Collection<String> otherKeys, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKeys, destKey);
    }


    public static  Set<String> members(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.members(key);
    }


    public static  String randomMember(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMember(key);
    }


    public static  Set<String> distinctRandomMembers(String key, long count) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.distinctRandomMembers(key, count);
    }


    public static  List<String> randomMembers(String key, long count) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMembers(key, count);
    }

    public static  Cursor<String> scan(String key, ScanOptions options) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.scan(key, options);
    }

    public static  RedisOperations getOperations() {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        return setOperations.getOperations();
    }



}
