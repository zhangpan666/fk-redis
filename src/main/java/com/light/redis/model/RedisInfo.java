package com.light.redis.model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class RedisInfo implements Serializable {

    private String name;

    private String keyPrefix;

    private int second;

    private int minute;

    private int hour;

    private int day;

    private int dbIndex;

    private long expire;


    public String getKey(Object... values) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(keyPrefix);
        for (Object object : values) {
            if (object != null) {
                stringBuilder.append("_").append(object.toString());
            }
        }
        return stringBuilder.toString();
    }

    public long getExpire() {
        return (this.second + this.minute * 60 + this.hour * 60 * 60 + this.day * 24 * 60 * 60) * 1000L;
    }

    private void setExpire(long expire) {
        this.expire = expire;
    }

}

