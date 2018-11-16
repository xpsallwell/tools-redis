package com.xps.tools.redis.exceptions;


public enum RedisToolsExceptionComp implements IExceptionComp{

	PARAMETER_FAIL_POOL_TYPE_JEDIS("R001","参数格式错误：Pool<Jedis>目前只支持JedisPool和JedisSentinelPool", IExceptionComp.Level.ERROR),
	PARAMETER_FAIL_POOL_TYPE_SHARDED("R002","参数格式错误：Pool<ShardedJedis>目前只支持ShardedJedisPool", IExceptionComp.Level.ERROR),
	NULL_OR_EMPTY_CONFIG_JEDISPOOL("R003","配置不能为空：redisPoolConfig或redisSentinelPoolConfig不能为空", IExceptionComp.Level.ERROR),
	NULL_OR_EMPTY_CONFIG_SHARDPOOL("R003","配置不能为空：redisShardedPoolConfig不能为空", IExceptionComp.Level.ERROR),
	NULL_OR_EMPTY_CONFIG_SHARDINFO("R004","配置不能为空：redisShardedPoolConfig中的shards不能为空", IExceptionComp.Level.ERROR),
	PARAM_NULL_HOST("R005","参数不能为空：HOST的值不能为空", IExceptionComp.Level.ERROR),
	PARAM_NULL_MASTERNAME("R006","参数不能为空：masterName的值不能为空", IExceptionComp.Level.ERROR),
	PARAM_NULL_SENTINELS("R007","参数不能为空：sentinels的值不能为空", IExceptionComp.Level.ERROR);

	
	private String code;
	private String message;
	private IExceptionComp.Level level;

	private RedisToolsExceptionComp(String code, String message, IExceptionComp.Level level) {
		this.code = code;
		this.message = message;
		this.level = level;
	}


	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getMsg() {
		return this.message;
	}


	@Override
	public Level getLevel() {
		return this.level;
	}

}
