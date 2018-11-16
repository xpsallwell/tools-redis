package com.xps.tools.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Set;

/**
 * Created by xiongps on 2018/5/30.
 */
public class RedisSentinelPoolConfig extends GenericObjectPoolConfig {


    private String masterName;
    //ip:port
    private Set<String> sentinels;
    private String password;

    /**connectionTimeout与soTimeout共用此超时时间*/
    private int timeout;
    private int connectionTimeout;
    private int soTimeout;

    private int database;
    private String clientName;

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public Set<String> getSentinels() {
        return sentinels;
    }

    public void setSentinels(Set<String> sentinels) {
        this.sentinels = sentinels;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }
}
