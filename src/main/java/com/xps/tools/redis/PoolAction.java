package com.xps.tools.redis;

import redis.clients.util.Pool;

/**
 * Created by xiongps on 2018/5/31.
 */
public interface PoolAction<T> {

    public Pool<T> getPool();
}
