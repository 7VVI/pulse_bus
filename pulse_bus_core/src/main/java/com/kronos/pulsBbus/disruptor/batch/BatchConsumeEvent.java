package com.kronos.pulsBbus.disruptor.batch;

import com.kronos.pulsBbus.core.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:27
 * @desc
 */
public class BatchConsumeEvent {
    private List<Message> messages = new ArrayList<>();
    private String topic;
    private long timestamp;
    private boolean processed = false;

    public BatchConsumeEvent() {
        this.messages = new ArrayList<>();
    }

    public void reset() {
        this.messages.clear();
        this.topic = null;
        this.timestamp = 0;
        this.processed = false;
    }

    public void addMessage(Message message) {
        this.messages.add(message);
        if (this.topic == null) {
            this.topic = message.getTopic();
        }
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and Setters
    public List<Message> getMessages() { return messages; }
    public void setMessages(List<Message> messages) { this.messages = messages; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public boolean isProcessed() { return processed; }
    public void setProcessed(boolean processed) { this.processed = processed; }

    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
