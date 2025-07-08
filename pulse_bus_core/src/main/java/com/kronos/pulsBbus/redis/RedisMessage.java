package com.kronos.pulsBbus.redis;

import java.io.Serializable;

/**
 * Redis消息对象
 * 用于在Redis中存储和传输消息
 */
public class RedisMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String messageId;
    private Object payload;
    private long timestamp;
    private int retryCount;
    
    public RedisMessage() {
    }
    
    public RedisMessage(String messageId, Object payload, long timestamp) {
        this.messageId = messageId;
        this.payload = payload;
        this.timestamp = timestamp;
        this.retryCount = 0;
    }
    
    public RedisMessage(String messageId, Object payload, long timestamp, int retryCount) {
        this.messageId = messageId;
        this.payload = payload;
        this.timestamp = timestamp;
        this.retryCount = retryCount;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public Object getPayload() {
        return payload;
    }
    
    public void setPayload(Object payload) {
        this.payload = payload;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
    
    @Override
    public String toString() {
        return "RedisMessage{" +
                "messageId='" + messageId + '\'' +
                ", payload='" + payload + '\'' +
                ", timestamp=" + timestamp +
                ", retryCount=" + retryCount +
                '}';
    }
}