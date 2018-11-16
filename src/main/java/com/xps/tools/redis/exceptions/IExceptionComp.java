package com.xps.tools.redis.exceptions;

/**
 * Created by xiongps on 2018/5/25.
 */
public interface IExceptionComp {

     String getCode();

     String getMsg();

    Level getLevel();

     enum Level {
        INFO, WARN, ERROR, FAIL;
    }
}
