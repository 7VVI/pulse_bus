package com.kronos.pulsBbus.disruptor.single;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:36
 * @desc
 */
public class DisruptorMessageEvent {
    private String              eventId;
    private String              topic;
    private Object              payload;
    private long                timestamp;
    private Map<String, Object> properties;

    public DisruptorMessageEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
        this.properties = new ConcurrentHashMap<>();
    }

    // 清空事件数据，用于对象复用
    public void clear() {
        this.eventId = null;
        this.topic = null;
        this.payload = null;
        this.timestamp = 0L;
        if (this.properties != null) {
            this.properties.clear();
        }
    }

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public Map<String, Object> getProperties() { return properties; }
    public void setProperties(Map<String, Object> properties) { this.properties = properties; }
}

