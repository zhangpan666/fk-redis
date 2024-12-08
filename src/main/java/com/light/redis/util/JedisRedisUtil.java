package com.light.redis.util;

import com.light.config.util.ApplicationContextUtil;
import com.light.redis.config.RedisPropertiesFactory;
import com.light.redis.model.RedisInfo;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@ConditionalOnClass({ApplicationContextUtil.class, RedisPropertiesFactory.class})
public class JedisRedisUtil {
    private static final RedisPropertiesFactory.RedisProperties redisProperties = RedisPropertiesFactory.getRedisProperties();
    private static final JedisPoolConfig jedisPoolConfig = jedisPoolConfig();
    private static final JedisShardInfo jedisShardInfo = jedisShardInfo();
    private static final ConcurrentHashMap<Integer, RedisTemplate> REDIS_TEMPLATE__MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, StringRedisTemplate> STRING_REDIS_TEMPLATE_MAP = new ConcurrentHashMap<>();


    public static <K, V> RedisTemplate<K, V> getRedisTemplate(int dbIndex) {
        RedisTemplate<K, V> redisTemplate = REDIS_TEMPLATE__MAP.get(dbIndex);
        if (redisTemplate != null) {
            return redisTemplate;
        }
        redisTemplate = RedisTemplateUtil.getCustomRedisTemplateWithDefaultValueSerializer(redisConnectionFactory(dbIndex));
        REDIS_TEMPLATE__MAP.put(dbIndex, redisTemplate);
        return redisTemplate;
    }


    public static StringRedisTemplate getStringRedisTemplate(int dbIndex) {
        StringRedisTemplate stringRedisTemplate = STRING_REDIS_TEMPLATE_MAP.get(dbIndex);
        if (stringRedisTemplate != null) {
            return stringRedisTemplate;
        }
        stringRedisTemplate = RedisTemplateUtil.getStringRedisTemplate(redisConnectionFactory(dbIndex));
        STRING_REDIS_TEMPLATE_MAP.put(dbIndex, stringRedisTemplate);
        return stringRedisTemplate;
    }


    private static JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(redisProperties.getMaxIdle());
        jedisPoolConfig.setMaxTotal(redisProperties.getMaxActive());
        jedisPoolConfig.setMinIdle(redisProperties.getMinIdle());
        jedisPoolConfig.setMaxWaitMillis(redisProperties.getMaxWait());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(redisProperties.getTimeEvictionRun());
        return jedisPoolConfig;
    }

    public static RedisConnectionFactory redisConnectionFactory(int dbIndex) {
        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
        jedisConnectionFactory.setShardInfo(jedisShardInfo);
        jedisConnectionFactory.setPoolConfig(jedisPoolConfig);
        jedisConnectionFactory.setPassword(redisProperties.getPassword());
        jedisConnectionFactory.setHostName(redisProperties.getHost());
        jedisConnectionFactory.setPort(redisProperties.getPort());
        jedisConnectionFactory.setDatabase(dbIndex);
        jedisConnectionFactory.setTimeout(redisProperties.getTimeout());
        jedisConnectionFactory.afterPropertiesSet();
        return jedisConnectionFactory;
    }

    private static JedisShardInfo jedisShardInfo() {
        JedisShardInfo jedisShardInfo = new JedisShardInfo(redisProperties.getHost(), redisProperties.getPort());
        jedisShardInfo.setPassword(redisProperties.getPassword());
        jedisShardInfo.setConnectionTimeout(redisProperties.getTimeout());
        return jedisShardInfo;
    }


    public static <K, V> RedisTemplate<K, V> getRedisTemplate() {
        return ApplicationContextUtil.getBean("redisTemplate", RedisTemplate.class);
    }

    public static StringRedisTemplate getStringRedisTemplate() {
        return ApplicationContextUtil.getBean("stringRedisTemplate", StringRedisTemplate.class);
    }


    public static <V> V getByRedisTemplate(String key, int dbIndex) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.get(key);
    }

    public static <V> V getByRedisTemplate(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.get(key);
    }

    public static <V> List<V> multiGetByRedisTemplate(String pattern, int dbIndex) {
        if (StringUtils.isEmpty(pattern)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> valueOperations = redisTemplate.opsForValue();
        Set<String> keys = redisTemplate.keys(pattern + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        return valueOperations.multiGet(keys);
    }

    public static <V> List<V> multiGetByRedisTemplate(String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            return null;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> valueOperations = redisTemplate.opsForValue();
        Set<String> keys = redisTemplate.keys(pattern + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        return valueOperations.multiGet(keys);
    }


    public static <V> boolean setByRedisTemplate(String key, V value, int dbIndex, long expire) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static <V> boolean setByRedisTemplate(String key, V value, long expire) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        opsForValue.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static <V> boolean setIfAbsentByRedisTemplate(String key, V value, int dbIndex) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate(dbIndex);
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.setIfAbsent(key, value);
    }

    public static <V> boolean setIfAbsentByRedisTemplate(String key, V value) {
        if (StringUtils.isEmpty(key) || value == null) {
            return false;
        }
        RedisTemplate<String, V> redisTemplate = getRedisTemplate();
        ValueOperations<String, V> opsForValue = redisTemplate.opsForValue();
        return opsForValue.setIfAbsent(key, value);
    }


    public static String getByStringRedisTemplate(String key, int dbIndex) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.get(key);
    }

    public static String getByStringRedisTemplate(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.get(key);
    }

    public static boolean setByStringRedisTemplate(String key, String value, int dbIndex, long expire) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        stringStringValueOperations.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static boolean setByStringRedisTemplate(String key, String value, long expire) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        valueOperations.set(key, value, expire, TimeUnit.MILLISECONDS);
        return true;
    }

    public static boolean setIfAbsentByStringRedisTemplate(String key, String value, int dbIndex) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
        return valueOperations.setIfAbsent(key, value);
    }

    public static boolean setIfAbsentByStringRedisTemplate(String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            return false;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate();
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        return stringStringValueOperations.setIfAbsent(key, value);
    }

    public static List<String> multiGetByStringRedisTemplate(String key, int dbIndex) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = getStringRedisTemplate(dbIndex);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        Set<String> keys = stringRedisTemplate.keys(key);
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        return stringStringValueOperations.multiGet(keys);
    }

    public static List<String> multiGetByStringRedisTemplate(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        StringRedisTemplate stringRedisTemplate = ApplicationContextUtil.getBean(StringRedisTemplate.class);
        ValueOperations<String, String> stringStringValueOperations = stringRedisTemplate.opsForValue();
        Set<String> keys = stringRedisTemplate.keys(key);
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        return stringStringValueOperations.multiGet(keys);
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

