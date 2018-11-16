package com.xps.tools.redis;

import redis.clients.jedis.Jedis;

/**
 * Created by xiongps on 2018/5/30.
 */
public interface JedisAction<T> {

    public T doAction(Jedis jedis);
}
