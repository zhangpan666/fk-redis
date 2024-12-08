package com.light.redis.util;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.Collection;
import java.util.Set;

public class RedisStringZSetUtil {

    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeWithScores(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeWithScores(key, start, end);
    }

    public static  Set<String> reverseRange(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRange(key, start, end);
    }

    public static  Set<String> rangeByLex(String key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range, limit);
    }

    public static  Set<String> range(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.range(key, start, end);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> rangeWithScores(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeWithScores(key, start, end);
    }

    public static  Set<String> rangeByLex(String key, RedisZSetCommands.Range range, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range);
    }

    public static  Double incrementScore(String key, String value, double delta, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.incrementScore(key, value, delta);
    }

    public static  Boolean add(String key, String value, double score, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, value, score);
    }

    public static  Long add(String key, Set<ZSetOperations.TypedTuple<String>> tuples, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, tuples);
    }

    public static  Set<String> reverseRangeByScore(String key, double min, double max, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max);
    }

    public static  Set<String> reverseRangeByScore(String key, double min, double max, long offset, long count, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max, offset, count);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> rangeByScoreWithScores(String key, final double min, final double max, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByScoreWithScores(key, min, max);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeByScoreWithScores(String key, double min, double max, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeByScoreWithScores(String key, double min, double max, long offset, long count, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max, offset, count);
    }

    public static  Long count(String key, double min, double max, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.count(key, min, max);
    }

    public static  Long intersectAndStore(String key, String otherKey, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static  Long intersectAndStore(String key, Collection<String> otherKeys, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static  Long rank(String key, Object o, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rank(key, o);
    }

    public static  Long reverseRank(String key, Object o, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRank(key, o);
    }

    public static  Long remove(int dbIndex, String key, Object... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.remove(key, values);
    }

    public static  Long removeRange(String key, long start, long end, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRange(key, start, end);
    }

    public static  Long removeRangeByScore(String key, double min, double max, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRangeByScore(key, min, max);
    }


    public static  Long size(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.size(key);
    }

    public static  Long unionAndStore(String key, String otherKey, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKey, destKey);
    }

    public static  Long unionAndStore(String key, Collection<String> otherKeys, String destKey, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static  Long zCard(String key, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.zCard(key);
    }

    public static  Double score(String key, Object o, int dbIndex) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate(dbIndex);
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.score(key, o);
    }





    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeWithScores(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeWithScores(key, start, end);
    }

    public static  Set<String> reverseRange(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRange(key, start, end);
    }

    public static  Set<String> rangeByLex(String key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range, limit);
    }

    public static  Set<String> range(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.range(key, start, end);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> rangeWithScores(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeWithScores(key, start, end);
    }

    public static  Set<String> rangeByLex(String key, RedisZSetCommands.Range range) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range);
    }

    public static  Double incrementScore(String key, String value, double delta) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.incrementScore(key, value, delta);
    }

    public static  Boolean add(String key, String value, double score) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, value, score);
    }

    public static  Long add(String key, Set<ZSetOperations.TypedTuple<String>> tuples) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, tuples);
    }

    public static  Set<String> reverseRangeByScore(String key, double min, double max) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max);
    }

    public static  Set<String> reverseRangeByScore(String key, double min, double max, long offset, long count) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max, offset, count);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> rangeByScoreWithScores(String key, final double min, final double max) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByScoreWithScores(key, min, max);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeByScoreWithScores(String key, double min, double max) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max);
    }

    public static  Set<ZSetOperations.TypedTuple<String>> reverseRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max, offset, count);
    }

    public static  Long count(String key, double min, double max) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.count(key, min, max);
    }

    public static  Long intersectAndStore(String key, String otherKey, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static  Long intersectAndStore(String key, Collection<String> otherKeys, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static  Long rank(String key, Object o) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rank(key, o);
    }

    public static  Long reverseRank(String key, Object o) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRank(key, o);
    }

    public static  Long remove(String key, Object... values) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.remove(key, values);
    }

    public static  Long removeRange(String key, long start, long end) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRange(key, start, end);
    }

    public static  Long removeRangeByScore(String key, double min, double max) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRangeByScore(key, min, max);
    }


    public static  Long size(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.size(key);
    }

    public static  Long unionAndStore(String key, String otherKey, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKey, destKey);
    }

    public static  Long unionAndStore(String key, Collection<String> otherKeys, String destKey) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static  Long zCard(String key) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.zCard(key);
    }

    public static  Double score(String key, Object o) {
        StringRedisTemplate redisTemplate = RedisStringUtil.getStringRedisTemplate();
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.score(key, o);
    }


}
