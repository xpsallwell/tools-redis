package com.xps.tools.redis;

import redis.clients.jedis.*;

import java.io.Closeable;

/**
 * Created by xiongps on 2018/5/30.
 */
public interface RedisClient extends JedisCommands,MultiKeyCommands,AdvancedJedisCommands,
        ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands,
        BinaryJedisCommands, MultiKeyBinaryCommands,BinaryScriptingCommands,AdvancedBinaryJedisCommands,Closeable {

    public <T> T execute(JedisAction<T> jedisAction);

    public void destroy();

}
