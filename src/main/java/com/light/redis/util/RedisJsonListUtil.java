package com.light.redis.util;

import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisJsonListUtil {

    public static <K, V> List<V> range(K key, long start, long end, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.range(key, start, end);
    }


    public static <K, V> void trim(K key, long start, long end, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        listOperations.trim(key, start, end);
    }

    public static <K, V> Long size(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.size(key);
    }


    public static <K, V> Long leftPush(K key, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static <K, V> Long leftPushAll(int dbIndex, K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static <K, V> Long leftPushAll(K key, Collection<V> values, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static <K, V> Long leftPushIfPresent(K key, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushIfPresent(key, value);
    }


    public static <K, V> Long leftPush(K key, V pivot, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, pivot, value);
    }

    public static <K, V> Long rightPush(K key, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static <K, V> Long rightPushAll(int dbIndex, K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static <K, V> Long rightPushAll(K key, Collection<V> values, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static <K, V> Long rightPushIfPresent(K key, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushIfPresent(key, value);
    }


    public static <K, V> Long rightPush(K key, V pivot, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPush(key, pivot, value);
    }


    public static <K, V> void set(K key, long index, V value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        listOperations.set(key, index, value);
    }


    public static <K, V> Long remove(K key, long count, Object value, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.remove(key, count, value);
    }


    public static <K, V> V index(K key, long index, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.index(key, index);
    }


    public static <K, V> V leftPop(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key);
    }


    public static <K, V> V leftPop(K key, long timeout, TimeUnit unit, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key, timeout, unit);
    }


    public static <K, V> V rightPop(K key, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key);
    }


    public static <K, V> V rightPop(K key, long timeout, TimeUnit unit, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key, timeout, unit);
    }


    public static <K, V> V rightPopAndLeftPush(K sourceKey, K destinationKey, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey);
    }

    public static <K, V> V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit, int dbIndex) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
    }




    public static <K, V> List<V> range(K key, long start, long end) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.range(key, start, end);
    }


    public static <K, V> void trim(K key, long start, long end) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        listOperations.trim(key, start, end);
    }

    public static <K, V> Long size(K key) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.size(key);
    }


    public static <K, V> Long leftPush(K key, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static <K, V> Long leftPushAll(K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static <K, V> Long leftPushAll(K key, Collection<V> values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static <K, V> Long leftPushIfPresent(K key, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushIfPresent(key, value);
    }


    public static <K, V> Long leftPush(K key, V pivot, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, pivot, value);
    }

    public static <K, V> Long rightPush(K key, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static <K, V> Long rightPushAll(K key, V... values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static <K, V> Long rightPushAll(K key, Collection<V> values) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static <K, V> Long rightPushIfPresent(K key, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushIfPresent(key, value);
    }


    public static <K, V> Long rightPush(K key, V pivot, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPush(key, pivot, value);
    }


    public static <K, V> void set(K key, long index, V value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        listOperations.set(key, index, value);
    }


    public static <K, V> Long remove(K key, long count, Object value) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.remove(key, count, value);
    }


    public static <K, V> V index(K key, long index) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.index(key, index);
    }


    public static <K, V> V leftPop(K key) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key);
    }


    public static <K, V> V leftPop(K key, long timeout, TimeUnit unit) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key, timeout, unit);
    }


    public static <K, V> V rightPop(K key) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key);
    }


    public static <K, V> V rightPop(K key, long timeout, TimeUnit unit) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key, timeout, unit);
    }


    public static <K, V> V rightPopAndLeftPush(K sourceKey, K destinationKey) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey);
    }

    public static <K, V> V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit) {
        RedisTemplate<K, V> redisTemplate = RedisJsonUtil.getRedisTemplate();
        ListOperations<K, V> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
    }
}
