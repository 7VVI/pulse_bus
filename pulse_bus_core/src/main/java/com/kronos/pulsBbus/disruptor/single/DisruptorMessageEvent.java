package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.Message;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:36
 * @desc
 */
public class DisruptorMessageEvent {
    private Message message;
    private String  topic;
    private long    timestamp;
    private boolean processed  = false;
    private int     retryCount = 0;

    public DisruptorMessageEvent() {
    }

    public void reset() {
        this.message = null;
        this.topic = null;
        this.timestamp = 0;
        this.processed = false;
        this.retryCount = 0;
    }

    public void set(String topic, Message message) {
        this.topic = topic;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
        this.processed = false;
        this.retryCount = 0;
    }
    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
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

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
}

