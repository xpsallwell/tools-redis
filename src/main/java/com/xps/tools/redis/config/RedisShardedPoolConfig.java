package com.xps.tools.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.util.Hashing;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by xiongps on 2018/5/30.
 */
public class RedisShardedPoolConfig extends GenericObjectPoolConfig {

    private List<JedisShardInfo> shards;
    private Hashing algo = Hashing.MURMUR_HASH;
    private Pattern keyTagPattern;

    public List<JedisShardInfo> getShards() {
        return shards;
    }

    public void setShards(List<JedisShardInfo> shards) {
        this.shards = shards;
    }

    public Hashing getAlgo() {
        return algo;
    }

    public void setAlgo(Hashing algo) {
        this.algo = algo;
    }

    public Pattern getKeyTagPattern() {
        return keyTagPattern;
    }

    public void setKeyTagPattern(Pattern keyTagPattern) {
        this.keyTagPattern = keyTagPattern;
    }
}
