package com.light.redis.util;

import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisStringListUtil {

    public static  List<String> range(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.range(key, start, end);
    }


    public static  void trim(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        listOperations.trim(key, start, end);
    }

    public static  Long size(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.size(key);
    }


    public static  Long leftPush(String key, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static  Long leftPushAll(int dbIndex, String key, String... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static  Long leftPushAll(String key, Collection<String> values, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static  Long leftPushIfPresent(String key, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushIfPresent(key, value);
    }


    public static  Long leftPush(String key, String pivot, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, pivot, value);
    }

    public static  Long rightPush(String key, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static  Long rightPushAll(int dbIndex, String key, String... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static  Long rightPushAll(String key, Collection<String> values, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static  Long rightPushIfPresent(String key, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushIfPresent(key, value);
    }


    public static  Long rightPush(String key, String pivot, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPush(key, pivot, value);
    }


    public static  void set(String key, long index, String value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        listOperations.set(key, index, value);
    }


    public static  Long remove(String key, long count, Object value, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.remove(key, count, value);
    }


    public static  String index(String key, long index, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.index(key, index);
    }


    public static  String leftPop(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key);
    }


    public static  String leftPop(String key, long timeout, TimeUnit unit, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key, timeout, unit);
    }


    public static  String rightPop(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key);
    }


    public static  String rightPop(String key, long timeout, TimeUnit unit, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key, timeout, unit);
    }


    public static  String rightPopAndLeftPush(String sourceKey, String destinationKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey);
    }

    public static  String rightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
    }




    public static  List<String> range(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.range(key, start, end);
    }


    public static  void trim(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        listOperations.trim(key, start, end);
    }

    public static  Long size(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.size(key);
    }


    public static  Long leftPush(String key, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static  Long leftPushAll(String key, String... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static  Long leftPushAll(String key, Collection<String> values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushAll(key, values);
    }


    public static  Long leftPushIfPresent(String key, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPushIfPresent(key, value);
    }


    public static  Long leftPush(String key, String pivot, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, pivot, value);
    }

    public static  Long rightPush(String key, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPush(key, value);
    }


    public static  Long rightPushAll(String key, String... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static  Long rightPushAll(String key, Collection<String> values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushAll(key, values);
    }


    public static  Long rightPushIfPresent(String key, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPushIfPresent(key, value);
    }


    public static  Long rightPush(String key, String pivot, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPush(key, pivot, value);
    }


    public static  void set(String key, long index, String value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        listOperations.set(key, index, value);
    }


    public static  Long remove(String key, long count, Object value) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.remove(key, count, value);
    }


    public static  String index(String key, long index) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.index(key, index);
    }


    public static  String leftPop(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key);
    }


    public static  String leftPop(String key, long timeout, TimeUnit unit) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.leftPop(key, timeout, unit);
    }


    public static  String rightPop(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key);
    }


    public static  String rightPop(String key, long timeout, TimeUnit unit) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPop(key, timeout, unit);
    }


    public static  String rightPopAndLeftPush(String sourceKey, String destinationKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey);
    }

    public static  String rightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        return listOperations.rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
    }


}
