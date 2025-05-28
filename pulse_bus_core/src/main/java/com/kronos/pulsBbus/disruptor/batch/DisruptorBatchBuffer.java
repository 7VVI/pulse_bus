package com.kronos.pulsBbus.disruptor.batch;

import com.kronos.pulsBbus.core.batch.BatchConsumeConfig;
import com.kronos.pulsBbus.core.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:27
 * @desc
 */
public class DisruptorBatchBuffer {

    private final    BatchConsumeConfig config;
    private final    List<Message>      buffer;
    private final    Object             lock = new Object();
    private volatile long               lastFlushTime;
    private final java.util.concurrent.ScheduledExecutorService scheduler;

    public DisruptorBatchBuffer(BatchConsumeConfig config) {
        this.config = config;
        this.buffer = new ArrayList<>();
        this.lastFlushTime = System.currentTimeMillis();
        this.scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "DisruptorBatchBuffer-Timer");
            t.setDaemon(true);
            return t;
        });

        // 启动定时刷新任务
        startFlushTimer();
    }

    /**
     * 添加消息到缓冲区
     */
    public List<Message> addMessage(Message message) {
        synchronized (lock) {
            buffer.add(message);

            // 检查是否需要立即刷新
            if (shouldFlush()) {
                return flushBuffer();
            }

            return null;
        }
    }

    /**
     * 检查是否应该刷新缓冲区
     */
    private boolean shouldFlush() {
        return buffer.size() >= config.getBatchSize() ||
                (System.currentTimeMillis() - lastFlushTime) >= config.getBatchTimeoutMs();
    }

    /**
     * 刷新缓冲区
     */
    private List<Message> flushBuffer() {
        if (buffer.isEmpty()) {
            return null;
        }

        List<Message> messages = new ArrayList<>(buffer);
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();

        return messages;
    }

    /**
     * 强制刷新缓冲区
     */
    public List<Message> forceFlush() {
        synchronized (lock) {
            return flushBuffer();
        }
    }

    /**
     * 启动定时刷新任务
     */
    private void startFlushTimer() {
        scheduler.scheduleAtFixedRate(() -> {
            List<Message> messages;
            synchronized (lock) {
                if (System.currentTimeMillis() - lastFlushTime >= config.getBatchTimeoutMs()) {
                    messages = flushBuffer();
                } else {
                    messages = null;
                }
            }

            if (messages != null && !messages.isEmpty()) {
                // 通知需要处理批量消息
                onBatchReady(messages);
            }
        }, config.getBatchTimeoutMs(), config.getBatchTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * 批量消息准备就绪回调
     */
    public void onBatchReady(List<Message> messages) {
        // 这个方法会被DisruptorBatchMessageTemplate调用
        System.out.println("定时器触发批量消息处理，消息数量: " + messages.size());
    }

    /**
     * 关闭缓冲器
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 获取当前缓冲区大小
     */
    public int size() {
        synchronized (lock) {
            return buffer.size();
        }
    }
}
