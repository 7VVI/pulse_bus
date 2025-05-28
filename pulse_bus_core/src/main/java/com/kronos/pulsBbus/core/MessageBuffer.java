package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.batch.BatchConsumeConfig;

import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:56
 * @desc 消息缓冲区
 */
public class MessageBuffer {
    private final java.util.concurrent.BlockingQueue<Message> buffer;
    private final BatchConsumeConfig                          config;

    public MessageBuffer(BatchConsumeConfig config) {
        this.config = config;
        this.buffer = new java.util.concurrent.LinkedBlockingQueue<>();
    }

    /**
     * 添加消息
     */
    public void addMessage(Message message) {
        try {
            buffer.offer(message, config.getMaxWaitTimeMs(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 批量取出消息
     */
    public List<Message> drainMessages(int maxSize) {
        List<Message> messages = new java.util.ArrayList<>();
        buffer.drainTo(messages, maxSize);
        return messages;
    }

    /**
     * 获取缓冲区大小
     */
    public int size() {
        return buffer.size();
    }

    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return buffer.isEmpty();
    }
}