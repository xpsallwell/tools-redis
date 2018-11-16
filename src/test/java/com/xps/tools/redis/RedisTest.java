package com.xps.tools.redis;

import com.xps.tools.redis.config.RedisPoolConfig;
import com.xps.tools.redis.config.RedisShardedPoolConfig;
import com.xps.tools.redis.impl.RedisClientImpl;
import com.xps.tools.redis.impl.ShardedRedisClientImpl;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Created by xiongps on 2018/5/31.
 */

public class RedisTest {
    private Logger logger = Logger.getLogger(this.getClass().getName());
    private static RedisPoolConfig redisPoolConfig;
    private static RedisShardedPoolConfig redisShardedPoolConfig;
    private static final int threadNum = 1000;
    private ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

    static {
        Properties properties = new Properties();
        InputStream is =RedisTest.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(properties.toString());
        redisPoolConfig = new RedisPoolConfig();
        redisPoolConfig.setHost(properties.getProperty("jedisPool.host"));
        redisPoolConfig.setPort(Integer.valueOf(properties.getProperty("jedisPool.port")));

        Properties properties2 = new Properties();
        InputStream is2 = RedisTest.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            properties2.load(is2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        redisShardedPoolConfig = new RedisShardedPoolConfig();
        List<JedisShardInfo> shardInfoList = new ArrayList<>();
        JedisShardInfo shardInfo = new JedisShardInfo(properties.getProperty("shardedJedisPool.shardInfo1.host"));
        shardInfoList.add(shardInfo);
        redisShardedPoolConfig.setShards(shardInfoList);

    }

    @Test
    public void testRedisClient(){
        //redisPoolConfig.setMaxWaitMillis(3l);
       // redisPoolConfig.setMaxTotal(15);
       // redisPoolConfig.setMaxIdle(8);
        redisPoolConfig.setTestOnBorrow(true);
        //redisPoolConfig.setTimeout(1000);
        RedisClientImpl redisClient = new RedisClientImpl(redisPoolConfig);
        //redisClient.setRedisPoolConfig(redisPoolConfig);
        try {
            Thread.sleep(5l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int idx = 1;
        for(int i=0;i<4000;i++){
            executorService.submit(new TestThread(redisClient,idx++));
        }
        executorService.shutdown();
        while (true){
            if(executorService.isTerminated()) {
                break;
            }
        }
        redisClient.destroy();
        logger.info("--结束---");
    }


    @Test
    public void testShardredisClient() {
        ShardedRedisClientImpl redisClient = new ShardedRedisClientImpl();
        //RedisShardedPoolConfig redisPoolConfig = this.getRedisShardedPoolConfig();
        /**
        List<JedisShardInfo> shardInfoList = new ArrayList<>();
        JedisShardInfo shardInfo = new JedisShardInfo("172.16.10.127");
        shardInfoList.add(shardInfo);
        redisPoolConfig.setShards(shardInfoList);
         */
        redisClient.setRedisShardedPoolConfig(redisShardedPoolConfig);

        final String key = "toolsRedisTestKey2";

       // redisClient.set(key,"{\"name\":\"李刚儿子\"}");
        System.out.println(redisClient.get(key));


        System.out.println(redisClient.get(key));

        redisClient.destroy();
    }

    private static RedisPoolConfig getRedisPoolConfig(){
        Properties properties = new Properties();
        InputStream is =RedisTest.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(properties.toString());
        RedisPoolConfig redisPoolConfig = new RedisPoolConfig();
        redisPoolConfig.setHost(properties.getProperty("jedisPool.host"));
        redisPoolConfig.setPort(Integer.valueOf(properties.getProperty("jedisPool.port")));
        return redisPoolConfig;
    }

    private static RedisShardedPoolConfig getRedisShardedPoolConfig(){
        Properties properties = new Properties();
        InputStream is = RedisTest.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        RedisShardedPoolConfig redisPoolConfig = new RedisShardedPoolConfig();



        List<JedisShardInfo> shardInfoList = new ArrayList<>();
        JedisShardInfo shardInfo = new JedisShardInfo(properties.getProperty("shardedJedisPool.shardInfo1.host"));

        shardInfoList.add(shardInfo);
        redisPoolConfig.setShards(shardInfoList);


        return redisPoolConfig;
    }

     class TestThread implements Runnable{
         private RedisClientImpl redisClient;
         private int idx;

         public TestThread(RedisClientImpl redisClient,int idx){
             this.redisClient = redisClient;
             this.idx = idx;

         }
         @Override
         public void run() {
             if(idx < 2){
                 //redisClient.setRedisPoolConfig(redisPoolConfig);
             }
             //RedisPoolConfig redisPoolConfig = RedisTest.getRedisPoolConfig();
             final String key = "toolsRedisTestKey1";
             //redisClient.set(key,"{\"name\":\"李刚\"}");
             logger.info("返回的："+redisClient.get(key)+idx);
             /**redisClient.execute(new JedisAction<Object>() {
                 @Override
                 public Object doAction(Jedis jedis) {
                     return jedis.del(key);
                 }
             });
              */
         }
     }
}
