package com.xps.tools.redis.impl;

import com.xps.tools.redis.util.PoolHandler;
import com.xps.tools.redis.ShardedJedisAction;
import com.xps.tools.redis.ShardedRedisClient;
import com.xps.tools.redis.config.RedisShardedPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.Pool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by xiongps on 2018/5/31.
 */
public class ShardedRedisClientImpl implements ShardedRedisClient {

    protected PoolHandler poolHandler = PoolHandler.getInstance();
    private static final String LOCK = "lock";
    private RedisShardedPoolConfig redisShardedPoolConfig;
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public ShardedRedisClientImpl(){}
    public ShardedRedisClientImpl(RedisShardedPoolConfig redisShardedPoolConfig){
        this.redisShardedPoolConfig = redisShardedPoolConfig;
        initGetShardPool();
    }

    public Pool<ShardedJedis> getShardedJedisPool(){
        Pool<ShardedJedis> pool = poolHandler.getShardedJedisPool();
        if(pool !=null) {
            return pool;
        }
        return initGetShardPool();
    }

    private Pool<ShardedJedis> initGetShardPool(){
        synchronized (LOCK) {//获取锁后，再次判断是否初始化完成，如果完成则直接返回
            if(poolHandler.getShardedJedisPool() !=null) {
                logger.info("线程["+Thread.currentThread().getName()+"]获取到LOCK时，已经初始化完成，直接返回");
                return poolHandler.getShardedJedisPool();
            }

            poolHandler.setRedisShardedPoolConfig(redisShardedPoolConfig, PoolHandler.INIT_DEFAULT_POOL_YES);
            logger.info("initGetShardPool初始化操作完成");
            return poolHandler.getShardedJedisPool();
        }
    }


    @Override
    public <T> T execute(ShardedJedisAction<T> shardedJedisAction) {
        Pool<ShardedJedis> shardedJedisPool = this.getShardedJedisPool();
        try(ShardedJedis shardedJedis = shardedJedisPool.getResource()){
            return shardedJedisAction.doAction(shardedJedis);
        }
    }

    @Override
    public void destroy() {
        Pool<ShardedJedis> shardedJedisPool = this.getShardedJedisPool();
        if(shardedJedisPool != null) {
            shardedJedisPool.close();
        }
    }

    @Override
    public void close() throws IOException {
        this.destroy();
    }

    public RedisShardedPoolConfig getRedisShardedPoolConfig() {
        return redisShardedPoolConfig;
    }

    public void setRedisShardedPoolConfig(RedisShardedPoolConfig redisShardedPoolConfig) {
        this.redisShardedPoolConfig = redisShardedPoolConfig;
        initGetShardPool();
    }

    @Override
    public String set(final String key, final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key,value);
            }
        });
    }

    @Override
    public String get(final String key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.get(key);
            }
        });
    }

    @Override
    public String set(final String key, final String value, final String nxxx, final String expx, final long time) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key,value,nxxx,expx,time);
            }
        });
    }

    @Override
    public String set(final String key, final String value, final String nxxx) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key,value,nxxx);
            }
        });
    }

    @Override
    public Boolean exists(final String key) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.exists(key);
            }
        });
    }

    @Override
    public Long persist(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.persist(key);
            }
        });
    }

    @Override
    public String type(final String key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.type(key);
            }
        });
    }

    @Override
    public Long expire(final String key, final int seconds) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expire(key,seconds);
            }
        });
    }

    @Override
    public Long pexpire(final String key, final long milliseconds) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pexpire(key,milliseconds);
            }
        });
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expireAt(key,unixTime);
            }
        });
    }

    @Override
    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pexpireAt(key,millisecondsTimestamp);
            }
        });
    }

    @Override
    public Long ttl(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.ttl(key);
            }
        });
    }

    @Override
    public Long pttl(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pttl(key);
            }
        });
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setbit(key,offset,value);
            }
        });
    }

    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setbit(key,offset,value);
            }
        });
    }

    @Override
    public Boolean getbit(final String key, final long offset) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getbit(key,offset);
            }
        });
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setrange(key,offset,value);
            }
        });
    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getrange(key,startOffset,endOffset);
            }
        });
    }

    @Override
    public String getSet(final String key, final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getSet(key,value);
            }
        });
    }

    @Override
    public Long setnx(final String key, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setnx(key,value);
            }
        });
    }

    @Override
    public String setex(final String key, final int seconds, final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setex(key,seconds,value);
            }
        });
    }

    @Override
    public String psetex(final String key, final long milliseconds, final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.psetex(key,milliseconds,value);
            }
        });
    }

    @Override
    public Long decrBy(final String key, final long integer) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.decrBy(key,integer);
            }
        });
    }

    @Override
    public Long decr(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.decr(key);
            }
        });
    }

    @Override
    public Long incrBy(final String key, final long integer) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incrBy(key,integer);
            }
        });
    }

    @Override
    public Double incrByFloat(final String key, final double integer) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incrByFloat(key,integer);
            }
        });
    }

    @Override
    public Long incr(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incr(key);
            }
        });
    }

    @Override
    public Long append(final String key, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.append(key,value);
            }
        });
    }

    @Override
    public String substr(final String key, final int start, final int end) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.substr(key,start,end);
            }
        });
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hset(key,field,value);
            }
        });
    }

    @Override
    public String hget(final String key, final String field) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hget(key,field);
            }
        });
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hsetnx(key,field,value);
            }
        });
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hmset(key,hash);
            }
        });
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hmget(key,fields);
            }
        });
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hincrBy(key,field,value);
            }
        });
    }

    @Override
    public Double hincrByFloat(final String key, final String field, final double value) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hincrByFloat(key,field,value);
            }
        });
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hexists(key,field);
            }
        });
    }

    @Override
    public Long hdel(final String key, final String... fields) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hdel(key,fields);
            }
        });
    }

    @Override
    public Long hlen(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hlen(key);
            }
        });
    }

    @Override
    public Set<String> hkeys(final String key) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hkeys(key);
            }
        });
    }

    @Override
    public List<String> hvals(final String key) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hvals(key);
            }
        });
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return this.execute(new ShardedJedisAction<Map<String, String>>() {
            @Override
            public Map<String, String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hgetAll(key);
            }
        });
    }

    @Override
    public Long rpush(final String key, final String... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpush(key,values);
            }
        });
    }

    @Override
    public Long lpush(final String key, final String... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpush(key,values);
            }
        });
    }

    @Override
    public Long llen(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.llen(key);
            }
        });
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lrange(key,start,end);
            }
        });
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.ltrim(key,start,end);
            }
        });
    }

    @Override
    public String lindex(final String key, final long index) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lindex(key,index);
            }
        });
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lset(key,index,value);
            }
        });
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lrem(key,count,value);
            }
        });
    }

    @Override
    public String lpop(final String keys) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpop(keys);
            }
        });
    }

    @Override
    public String rpop(final String key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpop(key);
            }
        });
    }

    @Override
    public Long sadd(final String key, final String... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sadd(key,members);
            }
        });
    }

    @Override
    public Set<String> smembers(final String key) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.smembers(key);
            }
        });
    }

    @Override
    public Long srem(final String key, final String... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srem(key,members);
            }
        });
    }

    @Override
    public String spop(final String key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.spop(key);
            }
        });
    }

    @Override
    public Set<String> spop(final String key, final long count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.spop(key,count);
            }
        });
    }

    @Override
    public Long scard(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.scard(key);
            }
        });
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sismember(key,member);
            }
        });
    }

    @Override
    public String srandmember(final String key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srandmember(key);
            }
        });
    }

    @Override
    public List<String> srandmember(final String key, final int count) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srandmember(key,count);
            }
        });
    }

    @Override
    public Long strlen(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.strlen(key);
            }
        });
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key,score,member);
            }
        });
    }

    @Override
    public Long zadd(final String key, final double score, final String member, final ZAddParams params) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key,score,member,params);
            }
        });
    }

    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key,scoreMembers);
            }
        });
    }

    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers, final ZAddParams params) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key,scoreMembers,params);
            }
        });
    }

    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrange(key,start,end);
            }
        });
    }

    @Override
    public Long zrem(final String key, final String... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrem(key,members);
            }
        });
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zincrby(key,score,member);
            }
        });
    }

    @Override
    public Double zincrby(final String key, final double score, final String member, final ZIncrByParams params) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zincrby(key,score,member,params);
            }
        });
    }

    @Override
    public Long zrank(final String key, final String member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrank(key,member);
            }
        });
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrank(key,member);
            }
        });
    }

    @Override
    public Set<String> zrevrange(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrange(key,start,end);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeWithScores(key,start,end);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeWithScores(key,start,end);
            }
        });
    }

    @Override
    public Long zcard(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcard(key);
            }
        });
    }

    @Override
    public Double zscore(final String key, final String member) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscore(key,member);
            }
        });
    }

    @Override
    public List<String> sort(final String key) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sort(key);
            }
        });
    }

    @Override
    public List<String> sort(final String key, final SortingParams sortingParameters) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sort(key,sortingParameters);
            }
        });
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcount(key,min,max);
            }
        });
    }

    @Override
    public Long zcount(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcount(key,min,max);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key,min,max);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key,min,max);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key,max,min);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key,min,max,offset,count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key,max,min);
            }
        });
    }

    @Override
    public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key,min,max,offset,count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key,max,min,offset,count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key,min,max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key,max,min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key,min,max,offset,count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key,max,min,offset,count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key,min,max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key,max,min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key,min,max,offset,count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key,max,min,offset,count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key,max,min,offset,count);
            }
        });
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByRank(key,start,end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByScore(key,start,end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final String key, final String start, final String end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByScore(key,start,end);
            }
        });
    }

    @Override
    public Long zlexcount(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zlexcount(key,min,max);
            }
        });
    }

    @Override
    public Set<String> zrangeByLex(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByLex(key,min,max);
            }
        });
    }

    @Override
    public Set<String> zrangeByLex(final String key, final String min, final String max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByLex(key,min,max,offset,count);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByLex(final String key, final String max, final String min) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByLex(key,max,min);
            }
        });
    }

    @Override
    public Set<String> zrevrangeByLex(final String key, final String max, final String min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<String>>() {
            @Override
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByLex(key,max,min,offset,count);
            }
        });
    }

    @Override
    public Long zremrangeByLex(final String key, final String min, final String max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByLex(key,min,max);
            }
        });
    }

    @Override
    public Long linsert(final String key, final BinaryClient.LIST_POSITION where, final String pivot, final String value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.linsert(key,where,pivot,value);
            }
        });
    }

    @Override
    public Long lpushx(final String key, final String... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpushx(key,values);
            }
        });
    }

    @Override
    public Long rpushx(final String key, final String... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpushx(key,values);
            }
        });
    }

    @Override
    public List<String> blpop(final String arg) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.blpop(arg);
            }
        });
    }

    @Override
    public List<String> blpop(final int timeout, final String key) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.blpop(timeout,key);
            }
        });
    }

    @Override
    public List<String> brpop(final String key) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.brpop(key);
            }
        });
    }

    @Override
    public List<String> brpop(final int timeout, final String key) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.brpop(timeout,key);
            }

        });
    }

    @Override
    public Long del(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.del(key);
            }
        });
    }

    @Override
    public String echo(final String value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.echo(value);
            }
        });
    }

    @Override
    public Long move(final String key, final int dbIndex) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.move(key,dbIndex);
            }
        });
    }

    @Override
    public Long bitcount(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitcount(key);
            }
        });
    }

    @Override
    public Long bitcount(final String key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitcount(key,start,end);
            }
        });
    }

    @Override
    public Long bitpos(final String key, final boolean value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitpos(key,value);
            }
        });
    }

    @Override
    public Long bitpos(final String key, final boolean value, final BitPosParams params) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitpos(key,value,params);
            }
        });
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final int cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Map.Entry<String, String>>>() {
            @Override
            public ScanResult<Map.Entry<String, String>> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final int cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<String>>() {
            @Override
            public ScanResult<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final int cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Map.Entry<String, String>>>() {
            @Override
            public ScanResult<Map.Entry<String, String>> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor, final ScanParams scanParams) {
        return this.execute(new ShardedJedisAction<ScanResult<Map.Entry<String, String>>>() {
            @Override
            public ScanResult<Map.Entry<String, String>> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hscan(key,cursor,scanParams);
            }
        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<String>>() {
            @Override
            public ScanResult<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams scanParams) {
        return this.execute(new ShardedJedisAction<ScanResult<String>>() {
            @Override
            public ScanResult<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sscan(key,cursor,scanParams);
            }
        });
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscan(key,cursor);
            }
        });
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams scanParams) {
        return this.execute(new ShardedJedisAction<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscan(key,cursor,scanParams);
            }
        });
    }

    @Override
    public Long pfadd(final String key, final String... elements) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pfadd(key,elements);
            }
        });
    }

    @Override
    public long pfcount(final String key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pfcount(key);
            }
        });
    }

    @Override
    public Long geoadd(final String key, final double longitude, final double latitude, final String member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geoadd(key,longitude,latitude,member);
            }
        });
    }

    @Override
    public Long geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geoadd(key,memberCoordinateMap);
            }
        });
    }

    @Override
    public Double geodist(final String key, final String member1, final String member2) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geodist(key,member1,member2);
            }
        });
    }

    @Override
    public Double geodist(final String key, final String member1, final String member2, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geodist(key,member1,member2,unit);
            }
        });
    }

    @Override
    public List<String> geohash(final String key, final String... members) {
        return this.execute(new ShardedJedisAction<List<String>>() {
            @Override
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geohash(key,members);
            }
        });
    }

    @Override
    public List<GeoCoordinate> geopos(final String key, final String... members) {
        return this.execute(new ShardedJedisAction<List<GeoCoordinate>>() {
            @Override
            public List<GeoCoordinate> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geopos(key,members);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadius(final String key, final double longitude, final double latitude, final double radius, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadius(key,longitude,latitude,radius,unit);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadius(final String key, final double longitude, final double latitude, final double radius, final GeoUnit geoUnit, final GeoRadiusParam geoRadiusParam) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadius(key,longitude,latitude,radius,geoUnit,geoRadiusParam);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final String key, final String member, final double radius, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadiusByMember(key,member,radius,unit);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final String key, final String member, final double radius, final GeoUnit geoUnit, final GeoRadiusParam geoRadiusParam) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadiusByMember(key,member,radius,geoUnit,geoRadiusParam);
            }
        });
    }

    @Override
    public List<Long> bitfield(final String key, final String... arguments) {
        return this.execute(new ShardedJedisAction<List<Long>>() {
            @Override
            public List<Long> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitfield(key,arguments);
            }
        });
    }










    @Override
    public String set(final byte[] key, final byte[] value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key, value);
            }
        });
    }

    @Override
    public String set(final byte[] key, final byte[] value, final byte[] nxxx) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key, value, nxxx);
            }
        });
    }

    @Override
    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.set(key, value, nxxx, expx, time);
            }
        });
    }

    @Override
    public byte[] get(final byte[] key) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.get(key);
            }
        });
    }

    @Override
    public Boolean exists(final byte[] key) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.exists(key);
            }
        });
    }

    @Override
    public Long persist(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.persist(key);
            }
        });
    }

    @Override
    public String type(final byte[] key) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.type(key);
            }
        });
    }

    @Override
    public Long expire(final byte[] key, final int seconds) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expire(key, seconds);
            }
        });
    }

    @Override
    public Long pexpire(final byte[] key, final long milliseconds) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pexpire(key, milliseconds);
            }
        });
    }

    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expireAt(key, unixTime);
            }
        });
    }

    @Override
    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pexpireAt(key, millisecondsTimestamp);
            }
        });
    }

    @Override
    public Long ttl(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.ttl(key);
            }
        });
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setbit(key, offset, value);
            }
        });
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setbit(key, offset, value);
            }
        });
    }

    @Override
    public Boolean getbit(final byte[] key, final long offset) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getbit(key, offset);
            }
        });
    }

    @Override
    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setrange(key, offset, value);
            }
        });
    }

    @Override
    public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getrange(key, startOffset, endOffset);
            }
        });
    }

    @Override
    public byte[] getSet(final byte[] key, final byte[] value) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.getSet(key, value);
            }
        });
    }

    @Override
    public Long setnx(final byte[] key, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setnx(key, value);
            }
        });
    }

    @Override
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.setex(key, seconds, value);
            }
        });
    }

    @Override
    public Long decrBy(final byte[] key, final long integer) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.decrBy(key, integer);
            }
        });
    }

    @Override
    public Long decr(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.decr(key);
            }
        });
    }

    @Override
    public Long incrBy(final byte[] key, final long integer) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incrBy(key, integer);
            }
        });
    }

    @Override
    public Double incrByFloat(final byte[] key, final double integer) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incrByFloat(key, integer);
            }
        });
    }

    @Override
    public Long incr(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.incr(key);
            }
        });
    }

    @Override
    public Long append(final byte[] key, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.append(key, value);
            }
        });
    }

    @Override
    public byte[] substr(final byte[] key, final int start, final int end) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.substr(key, start, end);
            }
        });
    }

    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hset(key, field, value);
            }
        });
    }

    @Override
    public byte[] hget(final byte[] key, final byte[] field) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hget(key, field);
            }
        });
    }

    @Override
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hsetnx(key, field, value);
            }
        });
    }

    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hmset(key, hash);
            }
        });
    }

    @Override
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hmget(key, fields);
            }
        });
    }

    @Override
    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hincrBy(key, field, value);
            }
        });
    }

    @Override
    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hincrByFloat(key, field, value);
            }
        });
    }

    @Override
    public Boolean hexists(final byte[] key, final byte[] field) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hexists(key, field);
            }
        });
    }

    @Override
    public Long hdel(final byte[] key, final byte[]... fields) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hdel(key, fields);
            }
        });
    }

    @Override
    public Long hlen(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hlen(key);
            }
        });
    }

    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hkeys(key);
            }
        });
    }

    @Override
    public Collection<byte[]> hvals(final byte[] key) {
        return this.execute(new ShardedJedisAction<Collection<byte[]>>() {
            @Override
            public Collection<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hvals(key);
            }
        });
    }

    @Override
    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return this.execute(new ShardedJedisAction<Map<byte[], byte[]>>() {
            @Override
            public Map<byte[], byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hgetAll(key);
            }
        });
    }

    @Override
    public Long rpush(final byte[] key, final byte[]... strings) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpush(key, strings);
            }
        });
    }

    @Override
    public Long lpush(final byte[] key, final byte[]... strings) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpush(key, strings);
            }
        });
    }

    @Override
    public Long llen(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.llen(key);
            }
        });
    }

    @Override
    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lrange(key, start, end);
            }
        });
    }

    @Override
    public String ltrim(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.ltrim(key, start, end);
            }
        });
    }

    @Override
    public byte[] lindex(final byte[] key, final long index) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lindex(key, index);
            }
        });
    }

    @Override
    public String lset(final byte[] key, final long index, final byte[] value) {
        return this.execute(new ShardedJedisAction<String>() {
            @Override
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lset(key, index, value);
            }
        });
    }

    @Override
    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lrem(key, count, value);
            }
        });
    }

    @Override
    public byte[] lpop(final byte[] key) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpop(key);
            }
        });
    }

    @Override
    public byte[] rpop(final byte[] key) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpop(key);
            }
        });
    }

    @Override
    public Long sadd(final byte[] key, final byte[]... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sadd(key, members);
            }
        });
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.smembers(key);
            }
        });
    }

    @Override
    public Long srem(final byte[] key, final byte[]... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srem(key, members);
            }
        });
    }

    @Override
    public byte[] spop(final byte[] key) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.spop(key);
            }
        });
    }

    @Override
    public Set<byte[]> spop(final byte[] key, final long count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.spop(key, count);
            }
        });
    }

    @Override
    public Long scard(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.scard(key);
            }
        });
    }

    @Override
    public Boolean sismember(final byte[] key, final byte[] member) {
        return this.execute(new ShardedJedisAction<Boolean>() {
            @Override
            public Boolean doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sismember(key, member);
            }
        });
    }

    @Override
    public byte[] srandmember(final byte[] key) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srandmember(key);
            }
        });
    }

    @Override
    public List<byte[]> srandmember(final byte[] key, final int count) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.srandmember(key, count);
            }
        });
    }

    @Override
    public Long strlen(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.strlen(key);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key, score, member);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final double score, final byte[] member, final ZAddParams params) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key, score, member, params);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key, scoreMembers);
            }
        });
    }

    @Override
    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers, final ZAddParams params) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key, scoreMembers, params);
            }
        });
    }

    @Override
    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrange(key, start, end);
            }
        });
    }

    @Override
    public Long zrem(final byte[] key, final byte[]... members) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrem(key, members);
            }
        });
    }

    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zincrby(key, score, member);
            }
        });
    }

    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member, final ZIncrByParams params) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zincrby(key, score, member, params);
            }
        });
    }

    @Override
    public Long zrank(final byte[] key, final byte[] member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrank(key, member);
            }
        });
    }

    @Override
    public Long zrevrank(final byte[] key, final byte[] member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrank(key, member);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrange(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeWithScores(key, start, end);
            }
        });
    }

    @Override
    public Long zcard(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcard(key);
            }
        });
    }

    @Override
    public Double zscore(final byte[] key, final byte[] member) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscore(key, member);
            }
        });
    }

    @Override
    public List<byte[]> sort(final byte[] key) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sort(key);
            }
        });
    }

    @Override
    public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sort(key, sortingParameters);
            }
        });
    }

    @Override
    public Long zcount(final byte[] key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcount(key, min, max);
            }
        });
    }

    @Override
    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zcount(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final double max, final double min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key, max, min);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScore(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScore(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key, min, max);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key, max, min);
            }
        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<Tuple>>() {
            @Override
            public Set<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByRank(key, start, end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByScore(key, start, end);
            }
        });
    }

    @Override
    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByScore(key, start, end);
            }
        });
    }

    @Override
    public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zlexcount(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByLex(key, min, max);
            }
        });
    }

    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrangeByLex(key, min, max, offset, count);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByLex(key, max, min);
            }
        });
    }

    @Override
    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min, final int offset, final int count) {
        return this.execute(new ShardedJedisAction<Set<byte[]>>() {
            @Override
            public Set<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrangeByLex(key, max, min, offset, count);
            }
        });
    }

    @Override
    public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zremrangeByLex(key, min, max);
            }
        });
    }

    @Override
    public Long linsert(final byte[] key, final BinaryClient.LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.linsert(key, where, pivot, value);
            }
        });
    }

    @Override
    public Long lpushx(final byte[] key, final byte[]... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lpushx(key, values);
            }
        });
    }

    @Override
    public Long rpushx(final byte[] key, final byte[]... values) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.rpushx(key, values);
            }
        });
    }

    @Override
    public List<byte[]> blpop(final byte[] arg) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.blpop(arg);
            }
        });
    }

    @Override
    public List<byte[]> brpop(final byte[] arg) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.brpop(arg);
            }
        });
    }

    @Override
    public Long del(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.del(key);
            }
        });
    }

    @Override
    public byte[] echo(final byte[] value) {
        return this.execute(new ShardedJedisAction<byte[]>() {
            @Override
            public byte[] doAction(ShardedJedis shardedJedis) {
                return shardedJedis.echo(value);
            }
        });
    }

    @Override
    public Long move(final byte[] key, final int dbIndex) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.move(key, dbIndex);
            }
        });
    }

    @Override
    public Long bitcount(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitcount(key);
            }
        });
    }

    @Override
    public Long bitcount(final byte[] key, final long start, final long end) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitcount(key, start, end);
            }
        });
    }

    @Override
    public Long pfadd(final byte[] key, final byte[]... elements) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pfadd(key, elements);
            }
        });
    }

    @Override
    public long pfcount(final byte[] key) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.pfcount(key);
            }
        });
    }

    @Override
    public Long geoadd(final byte[] key, final double longitude, final double latitude, final byte[] member) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geoadd(key, longitude, latitude, member);
            }
        });
    }

    @Override
    public Long geoadd(final byte[] key, final Map<byte[], GeoCoordinate> memberCoordinateMap) {
        return this.execute(new ShardedJedisAction<Long>() {
            @Override
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geoadd(key, memberCoordinateMap);
            }
        });
    }

    @Override
    public Double geodist(final byte[] key, final byte[] member1, final byte[] member2) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geodist(key, member1, member2);
            }
        });
    }

    @Override
    public Double geodist(final byte[] key, final byte[] member1, final byte[] member2, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<Double>() {
            @Override
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geodist(key, member1, member2, unit);
            }
        });
    }

    @Override
    public List<byte[]> geohash(final byte[] key, final byte[]... members) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geohash(key, members);
            }
        });
    }

    @Override
    public List<GeoCoordinate> geopos(final byte[] key, final byte[]... members) {
        return this.execute(new ShardedJedisAction<List<GeoCoordinate>>() {
            @Override
            public List<GeoCoordinate> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.geopos(key, members);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude, final double latitude, final double radius, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadius(key, longitude, latitude, radius, unit);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadius(final byte[] key, final double longitude, final double latitude, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadius(key, longitude, latitude, radius, unit, param);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member, final double radius, final GeoUnit unit) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadiusByMember(key, member, radius, unit);
            }
        });
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(final byte[] key, final byte[] member, final double radius, final GeoUnit unit, final GeoRadiusParam param) {
        return this.execute(new ShardedJedisAction<List<GeoRadiusResponse>>() {
            @Override
            public List<GeoRadiusResponse> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.georadiusByMember(key, member, radius, unit, param);
            }
        });
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Map.Entry<byte[], byte[]>>>() {
            @Override
            public ScanResult<Map.Entry<byte[], byte[]>> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hscan(key, cursor);
            }
        });
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.execute(new ShardedJedisAction<ScanResult<Map.Entry<byte[], byte[]>>>() {
            @Override
            public ScanResult<Map.Entry<byte[], byte[]>> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hscan(key, cursor, params);
            }
        });
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<byte[]>>() {
            @Override
            public ScanResult<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sscan(key, cursor);
            }
        });
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.execute(new ShardedJedisAction<ScanResult<byte[]>>() {
            @Override
            public ScanResult<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.sscan(key, cursor, params);
            }
        });
    }

    @Override
    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
        return this.execute(new ShardedJedisAction<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscan(key, cursor);
            }
        });
    }

    @Override
    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return this.execute(new ShardedJedisAction<ScanResult<Tuple>>() {
            @Override
            public ScanResult<Tuple> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscan(key, cursor, params);
            }
        });
    }

    @Override
    public List<byte[]> bitfield(final byte[] key, final byte[]... arguments) {
        return this.execute(new ShardedJedisAction<List<byte[]>>() {
            @Override
            public List<byte[]> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.bitfield(key, arguments);
            }
        });
    }
}
