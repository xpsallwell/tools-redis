package com.xps.tools.redis.config;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by xiongps on 2018/5/30.
 */
public class RedisPoolConfig extends JedisPoolConfig {

    public RedisPoolConfig(){
        super();
    }

    private String host;

    private int port = 6379;

    private int timeout = 2000;

    private String password;
    private boolean ssl = false;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }
}
