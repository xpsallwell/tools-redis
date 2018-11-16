package com.xps.tools.redis.util;

import com.xps.tools.redis.PoolAction;
import com.xps.tools.redis.config.RedisPoolConfig;
import com.xps.tools.redis.config.RedisSentinelPoolConfig;
import com.xps.tools.redis.config.RedisShardedPoolConfig;
import com.xps.tools.redis.exceptions.RedisToolsException;
import com.xps.tools.redis.exceptions.RedisToolsExceptionComp;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

/**
 * Created by xiongps on 2018/5/30.
 */
public class PoolHandler {

    private static final String LOCK = "lock";
    private static PoolHandler instance = new PoolHandler();
    private JedisPool jedisPool = null;
    private JedisSentinelPool jedisSentinelPool = null;
    private ShardedJedisPool shardedJedisPool = null;

    private RedisPoolConfig redisPoolConfig = null;
    private RedisSentinelPoolConfig redisSentinelPoolConfig = null;
    private RedisShardedPoolConfig redisShardedPoolConfig = null;

    public static final boolean INIT_DEFAULT_POOL_YES = true;
    public static final boolean INIT_DEFAULT_POOL_NO = false;

    private PoolHandler(){

    }

    public static PoolHandler getInstance(){
        return instance;
    }

    public Pool<Jedis> getJedisPool(){
        return jedisPool==null?jedisSentinelPool:jedisPool;
    }

    public Pool<ShardedJedis> getShardedJedisPool(){
        return shardedJedisPool;
    }

    /**
     * 非线程安全
     * @throws RedisToolsException
     */
    private void initJedisPool() throws RedisToolsException{
        if(jedisPool!=null){
            return;
        }
        if(redisPoolConfig != null) {
            this.checkRedisPoolConfig(redisPoolConfig);
            Pool<Jedis> pool = this.createPool(new PoolAction<Jedis>() {
                @Override
                public Pool<Jedis> getPool() {
                    JedisPool  jp = new JedisPool(redisPoolConfig,redisPoolConfig.getHost(),
                            redisPoolConfig.getPort(),
                            redisPoolConfig.getTimeout(),redisPoolConfig.getPassword(),redisPoolConfig.isSsl());
                    return jp;
                }
            });
            this.setJedisPool(pool);
        }
    }

    /**
     * 非线程安全
     * @throws RedisToolsException
     */
    private void initJedisSentinelPool() throws RedisToolsException{
        if(jedisSentinelPool != null){
            return;
        }
        if(redisSentinelPoolConfig != null){
            this.checkRedisSentinelPoolConfig(redisSentinelPoolConfig);
            Pool<Jedis> pool = this.createPool(new PoolAction<Jedis>() {
                @Override
                public Pool<Jedis> getPool() {
                    JedisSentinelPool jsp = new JedisSentinelPool(redisSentinelPoolConfig.getMasterName(),
                            redisSentinelPoolConfig.getSentinels(),redisSentinelPoolConfig,redisSentinelPoolConfig.getConnectionTimeout(),
                            redisSentinelPoolConfig.getSoTimeout(),redisSentinelPoolConfig.getPassword(),redisSentinelPoolConfig.getDatabase(),redisSentinelPoolConfig.getClientName());
                    return jsp;
                }
            });
            this.setJedisPool(pool);
        }
    }

    /**
     * 非线程安全
     * @throws RedisToolsException
     */
    private void initShardedJedisPool() throws RedisToolsException{
        if(shardedJedisPool != null ) {
            return;
        }
        this.checkRedisShardedPoolConfig(redisShardedPoolConfig);
        Pool<ShardedJedis> pool = this.createPool(new PoolAction<ShardedJedis>() {
            @Override
            public Pool<ShardedJedis> getPool() {
                Pool<ShardedJedis> pool = new ShardedJedisPool(redisShardedPoolConfig,
                        redisShardedPoolConfig.getShards(), redisShardedPoolConfig.getAlgo(), redisShardedPoolConfig.getKeyTagPattern());
                return pool;
            }
        });
        this.setShardedJedisPool(pool);

    }

    private void checkRedisShardedPoolConfig(RedisShardedPoolConfig redisShardedPoolConfig) {
        if(redisShardedPoolConfig == null) {
            throw new RedisToolsException(RedisToolsExceptionComp.NULL_OR_EMPTY_CONFIG_SHARDPOOL);
        }
        if(redisShardedPoolConfig.getShards()== null || redisShardedPoolConfig.getShards().size() <=0) {
            throw new RedisToolsException(RedisToolsExceptionComp.NULL_OR_EMPTY_CONFIG_SHARDINFO);
        }
        //校验参数的格式

    }

    private void checkRedisSentinelPoolConfig(RedisSentinelPoolConfig redisSentinelPoolConfig)
        throws RedisToolsException{
        if(redisSentinelPoolConfig.getMasterName() == null || "".equals(redisSentinelPoolConfig.getMasterName())) {
            throw new RedisToolsException(RedisToolsExceptionComp.PARAM_NULL_MASTERNAME);
        }
        if(redisSentinelPoolConfig.getSentinels()== null || redisSentinelPoolConfig.getSentinels().size() <=0) {
            throw new RedisToolsException(RedisToolsExceptionComp.PARAM_NULL_SENTINELS);
        }
        //校验参数的格式

    }

    private void checkRedisPoolConfig(RedisPoolConfig redisPoolConfig)
            throws RedisToolsException{
        if(redisPoolConfig.getHost() == null || "".equals(redisPoolConfig.getHost())) {
            throw new RedisToolsException(RedisToolsExceptionComp.PARAM_NULL_HOST);
        }
        //校验参数的格式
    }

    public <T> Pool<T> createPool(PoolAction<T> poolAction){
        return poolAction.getPool();
    }

    public void setJedisPool(Pool<Jedis> pool) {
        if(pool instanceof JedisPool) {
            this.jedisPool = (JedisPool)pool;
        } else if(pool instanceof JedisSentinelPool) {
            this.jedisSentinelPool = (JedisSentinelPool)pool;
        } else {
            throw new RedisToolsException(RedisToolsExceptionComp.PARAMETER_FAIL_POOL_TYPE_JEDIS);
        }
    }

    public void setShardedJedisPool(Pool<ShardedJedis> pool) {
        if(pool instanceof ShardedJedisPool) {
            this.shardedJedisPool = (ShardedJedisPool)pool;
        }else {
            throw new RedisToolsException(RedisToolsExceptionComp.PARAMETER_FAIL_POOL_TYPE_SHARDED);
        }
    }

    public RedisPoolConfig getRedisPoolConfig() {
        return redisPoolConfig;
    }

    public void setRedisPoolConfig(RedisPoolConfig redisPoolConfig,boolean isInitDefaultPool) {
        this.redisPoolConfig = redisPoolConfig;
        if(isInitDefaultPool) {
            this.initJedisPool();
        }
    }

    public RedisSentinelPoolConfig getRedisSentinelPoolConfig() {
        return redisSentinelPoolConfig;
    }

    public void setRedisSentinelPoolConfig(RedisSentinelPoolConfig redisSentinelPoolConfig,boolean isInitDefaultPool) {
        this.redisSentinelPoolConfig = redisSentinelPoolConfig;
        if(isInitDefaultPool) {
            this.initJedisSentinelPool();
        }
    }

    public RedisShardedPoolConfig getRedisShardedPoolConfig() {
        return redisShardedPoolConfig;
    }

    public void setRedisShardedPoolConfig(RedisShardedPoolConfig redisShardedPoolConfig,boolean isInitDefaultPool) {
        this.redisShardedPoolConfig = redisShardedPoolConfig;
        if(isInitDefaultPool) {
            this.initShardedJedisPool();
        }
    }
}
