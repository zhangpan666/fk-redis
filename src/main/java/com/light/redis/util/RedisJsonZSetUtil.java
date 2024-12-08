package com.light.redis.util;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.Collection;
import java.util.Set;

public class RedisJsonZSetUtil {

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeWithScores(K key, long start, long end, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeWithScores(key, start, end);
    }

    public static <K, V> Set<V> reverseRange(K key, long start, long end, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRange(key, start, end);
    }

    public static <K, V> Set<V> rangeByLex(K key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range, limit);
    }

    public static <K, V> Set<V> range(K key, long start, long end, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.range(key, start, end);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> rangeWithScores(K key, long start, long end, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeWithScores(key, start, end);
    }

    public static <K, V> Set<V> rangeByLex(K key, RedisZSetCommands.Range range, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range);
    }

    public static <K, V> Double incrementScore(K key, V value, double delta, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.incrementScore(key, value, delta);
    }

    public static <K, V> Boolean add(K key, V value, double score, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, value, score);
    }

    public static <K, V> Long add(K key, Set<ZSetOperations.TypedTuple<V>> tuples, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, tuples);
    }

    public static <K, V> Set<V> reverseRangeByScore(K key, double min, double max, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max);
    }

    public static <K, V> Set<V> reverseRangeByScore(K key, double min, double max, long offset, long count, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max, offset, count);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> rangeByScoreWithScores(K key, final double min, final double max, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByScoreWithScores(key, min, max);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, long offset, long count, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max, offset, count);
    }

    public static <K, V> Long count(K key, double min, double max, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.count(key, min, max);
    }

    public static <K, V> Long intersectAndStore(K key, K otherKey, K destKey, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long intersectAndStore(K key, Collection<K> otherKeys, K destKey, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Long rank(K key, Object o, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rank(key, o);
    }

    public static <K, V> Long reverseRank(K key, Object o, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRank(key, o);
    }

    public static <K, V> Long remove(int dbIndex, K key, Object... values) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.remove(key, values);
    }

    public static <K, V> Long removeRange(K key, long start, long end, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRange(key, start, end);
    }

    public static <K, V> Long removeRangeByScore(K key, double min, double max, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRangeByScore(key, min, max);
    }


    public static <K, V> Long size(K key, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.size(key);
    }

    public static <K, V> Long unionAndStore(K key, K otherKey, K destKey, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long unionAndStore(K key, Collection<K> otherKeys, K destKey, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Long zCard(K key, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.zCard(key);
    }

    public static <K, V> Double score(K key, Object o, int dbIndex) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate(dbIndex);
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.score(key, o);
    }






    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeWithScores(K key, long start, long end) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeWithScores(key, start, end);
    }

    public static <K, V> Set<V> reverseRange(K key, long start, long end) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRange(key, start, end);
    }

    public static <K, V> Set<V> rangeByLex(K key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range, limit);
    }

    public static <K, V> Set<V> range(K key, long start, long end) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.range(key, start, end);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> rangeWithScores(K key, long start, long end) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeWithScores(key, start, end);
    }

    public static <K, V> Set<V> rangeByLex(K key, RedisZSetCommands.Range range) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByLex(key, range);
    }

    public static <K, V> Double incrementScore(K key, V value, double delta) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.incrementScore(key, value, delta);
    }

    public static <K, V> Boolean add(K key, V value, double score) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, value, score);
    }

    public static <K, V> Long add(K key, Set<ZSetOperations.TypedTuple<V>> tuples) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.add(key, tuples);
    }

    public static <K, V> Set<V> reverseRangeByScore(K key, double min, double max) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max);
    }

    public static <K, V> Set<V> reverseRangeByScore(K key, double min, double max, long offset, long count) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScore(key, min, max, offset, count);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> rangeByScoreWithScores(K key, final double min, final double max) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rangeByScoreWithScores(key, min, max);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max);
    }

    public static <K, V> Set<ZSetOperations.TypedTuple<V>> reverseRangeByScoreWithScores(K key, double min, double max, long offset, long count) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRangeByScoreWithScores(key, min, max, offset, count);
    }

    public static <K, V> Long count(K key, double min, double max) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.count(key, min, max);
    }

    public static <K, V> Long intersectAndStore(K key, K otherKey, K destKey) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.intersectAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Long rank(K key, Object o) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.rank(key, o);
    }

    public static <K, V> Long reverseRank(K key, Object o) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.reverseRank(key, o);
    }

    public static <K, V> Long remove(K key, Object... values) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.remove(key, values);
    }

    public static <K, V> Long removeRange(K key, long start, long end) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRange(key, start, end);
    }

    public static <K, V> Long removeRangeByScore(K key, double min, double max) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.removeRangeByScore(key, min, max);
    }


    public static <K, V> Long size(K key) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.size(key);
    }

    public static <K, V> Long unionAndStore(K key, K otherKey, K destKey) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKey, destKey);
    }

    public static <K, V> Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.unionAndStore(key, otherKeys, destKey);
    }

    public static <K, V> Long zCard(K key) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.zCard(key);
    }

    public static <K, V> Double score(K key, Object o) {
        RedisTemplate redisTemplate = RedisJsonUtil.getRedisTemplate();
        ZSetOperations<K, V> zSetOperations = redisTemplate.opsForZSet();
        return zSetOperations.score(key, o);
    }


}
