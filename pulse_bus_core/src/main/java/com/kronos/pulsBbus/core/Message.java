package com.kronos.pulsBbus.core;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:27
 * @desc
 */

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * 统一消息实体
 */
public class Message implements Serializable {
    private String              id;
    private String              topic;
    private String              tag;
    private String              consumerGroup;
    private Object              payload;
    private Map<String, Object> properties;
    private LocalDateTime       timestamp;
    private int                 retryCount;
    private long                delayTime;
    private MessageType         messageType;

    public enum MessageType {
        NORMAL, DELAY, TRANSACTION, ORDER
    }

    public Message() {
        this.id = java.util.UUID.randomUUID().toString();
        this.timestamp = LocalDateTime.now();
        this.messageType = MessageType.NORMAL;
        this.properties = new java.util.HashMap<>();
    }

    public Message(String topic, Object payload) {
        this();
        this.topic = topic;
        this.payload = payload;
    }

    // 全部getter/setter方法
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }
}

