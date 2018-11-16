package com.xps.tools.redis;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;

import java.io.Closeable;

/**
 * Created by xiongps on 2018/5/31.
 */
public interface ShardedRedisClient extends JedisCommands,BinaryJedisCommands,Closeable {

    public <T> T execute(ShardedJedisAction<T> shardedJedisAction);

    public void destroy();


}
