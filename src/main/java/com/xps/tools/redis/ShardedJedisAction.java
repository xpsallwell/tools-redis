package com.xps.tools.redis;

import redis.clients.jedis.ShardedJedis;

/**
 * Created by xiongps on 2018/5/30.
 */
public interface ShardedJedisAction<T> {

    public T doAction(ShardedJedis shardedJedis);
}
