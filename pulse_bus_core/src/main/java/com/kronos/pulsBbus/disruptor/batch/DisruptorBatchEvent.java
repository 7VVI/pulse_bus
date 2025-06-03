package com.kronos.pulsBbus.disruptor.batch;

import com.kronos.pulsBbus.core.Message;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:44
 * @desc
 */
public class DisruptorBatchEvent {
    private java.util.List<Message> messages  = new java.util.ArrayList<>();
    private String                  topic;
    private long                    timestamp;
    private boolean                 processed = false;

    public DisruptorBatchEvent() {
        this.messages = new java.util.ArrayList<>();
    }

    public void reset() {
        this.messages.clear();
        this.topic = null;
        this.timestamp = 0;
        this.processed = false;
    }

    public void setBatch(String topic, java.util.List<Message> messages) {
        this.topic = topic;
        this.messages = new java.util.ArrayList<>(messages);
        this.timestamp = System.currentTimeMillis();
        this.processed = false;
    }

    // Getters and Setters
    public java.util.List<Message> getMessages() {
        return messages;
    }

    public void setMessages(java.util.List<Message> messages) {
        this.messages = messages;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
