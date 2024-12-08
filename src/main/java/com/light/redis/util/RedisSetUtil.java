package com.light.redis.util;

import org.springframework.data.redis.core.*;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RedisSetUtil {

    public static <K, V> Long add(int dbIndex, K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.add(key, values);
    }

    public static <K, V> Long remove(int dbIndex, K key, Object... values) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.remove(key, values);
    }

    public static <K, V> V pop(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.pop(key);
    }

    public static <K, V> Boolean move(K key, V value, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.move(key, value, destKey);
    }


    public static <K, V> Long size(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.size(key);
    }

    public static <K, V> Boolean isMember(K key, Object o, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.isMember(key, o);
    }

    public static <K, V> Set<V> intersect(K key, K otherKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKey);
    }


    public static <K, V> Set<V> intersect(K key, Collection<K> otherKeys, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKeys);
    }

    public static <K, V> Long intersectAndStore(K key, K otherKey, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long intersectAndStore(K key, Collection<K> otherKeys, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Set<V> union(K key, K otherKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKey);
    }

    public static <K, V> Set<V> union(K key, Collection<K> otherKeys, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKeys);
    }


    public static <K, V> Long unionAndStore(K key, K otherKey, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKey, destKey);
    }


    public static <K, V> Long unionAndStore(K key, Collection<K> otherKeys, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Set<V> difference(K key, K otherKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKey);
    }

    public static <K, V> Set<V> difference(K key, Collection<K> otherKeys, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKeys);
    }


    public static <K, V> Long differenceAndStore(K key, K otherKey, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKey, destKey);
    }


    public static <K, V> Long differenceAndStore(K key, Collection<K> otherKeys, K destKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKeys, destKey);
    }


    public static <K, V> Set<V> members(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.members(key);
    }


    public static <K, V> V randomMember(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMember(key);
    }


    public static <K, V> Set<V> distinctRandomMembers(K key, long count, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.distinctRandomMembers(key, count);
    }


    public static <K, V> List<V> randomMembers(K key, long count, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMembers(key, count);
    }

    public static <K, V> Cursor<V> scan(K key, ScanOptions options, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.scan(key, options);
    }

    public static <K, V> RedisOperations<K, V> getOperations(int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate(dbIndex);
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.getOperations();
    }





    public static <K, V> Long add(K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.add(key, values);
    }

    public static <K, V> Long remove(K key, Object... values) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.remove(key, values);
    }

    public static <K, V> V pop(K key) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.pop(key);
    }

    public static <K, V> Boolean move(K key, V value, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.move(key, value, destKey);
    }


    public static <K, V> Long size(K key) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.size(key);
    }

    public static <K, V> Boolean isMember(K key, Object o) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.isMember(key, o);
    }

    public static <K, V> Set<V> intersect(K key, K otherKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKey);
    }


    public static <K, V> Set<V> intersect(K key, Collection<K> otherKeys) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersect(key, otherKeys);
    }

    public static <K, V> Long intersectAndStore(K key, K otherKey, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Set<V> union(K key, K otherKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKey);
    }

    public static <K, V> Set<V> union(K key, Collection<K> otherKeys) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.union(key, otherKeys);
    }


    public static <K, V> Long unionAndStore(K key, K otherKey, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKey, destKey);
    }


    public static <K, V> Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Set<V> difference(K key, K otherKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKey);
    }

    public static <K, V> Set<V> difference(K key, Collection<K> otherKeys) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.difference(key, otherKeys);
    }


    public static <K, V> Long differenceAndStore(K key, K otherKey, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKey, destKey);
    }


    public static <K, V> Long differenceAndStore(K key, Collection<K> otherKeys, K destKey) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.differenceAndStore(key, otherKeys, destKey);
    }


    public static <K, V> Set<V> members(K key) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.members(key);
    }


    public static <K, V> V randomMember(K key) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMember(key);
    }


    public static <K, V> Set<V> distinctRandomMembers(K key, long count) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.distinctRandomMembers(key, count);
    }


    public static <K, V> List<V> randomMembers(K key, long count) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.randomMembers(key, count);
    }

    public static <K, V> Cursor<V> scan(K key, ScanOptions options) {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.scan(key, options);
    }

    public static <K, V> RedisOperations<K, V> getOperations() {
        RedisTemplate<K, V> redisTemplate = RedisUtil.getRedisTemplate();
        SetOperations<K, V> setOperations = redisTemplate.opsForSet();
        return setOperations.getOperations();
    }



}
