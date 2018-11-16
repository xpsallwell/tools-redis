package com.xps.tools.redis.exceptions;

import java.text.MessageFormat;

public class RedisToolsException extends RuntimeException {

	private static final long serialVersionUID = -19856789908383294L;
	
	private String code;
	
	private String message;
	
	private IExceptionComp exceptionComp;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public RedisToolsException(String message){
		this.message = message;
	}
	
	public RedisToolsException(IExceptionComp exceptionComp){
		this.exceptionComp = exceptionComp;
	}
	
	public RedisToolsException(IExceptionComp enumException, Object...arguments){
		this.exceptionComp = enumException;
		this.message = MessageFormat.format(this.exceptionComp.getMsg(), arguments);
	}
	
	public RedisToolsException(String code, String message){
		 super(message);
		 this.code = code;
		 this.message = message;
	}
	
	public RedisToolsException(String message, Throwable cause){
		 super(message,new Throwable(message));
		 this.message = message;
	}

	public IExceptionComp getEnumException() {
		return exceptionComp;
	}

	public void setEnumException(IExceptionComp exceptionComp) {
		this.exceptionComp = exceptionComp;
	}
	
	
}
