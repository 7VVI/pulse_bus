package com.kronos.pulsBbus.disruptor;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.batch.BatchConsumeResult;
import com.kronos.pulsBbus.disruptor.batch.DisruptorBatchEvent;
import com.kronos.pulsBbus.disruptor.single.DisruptorMessageEvent;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:46
 * @desc
 */
@org.springframework.stereotype.Component
public class DisruptorRetryHandler {

    private final DisruptorProperties properties;
    private final java.util.concurrent.ScheduledExecutorService retryExecutor;

    public DisruptorRetryHandler(DisruptorProperties properties) {
        this.properties = properties;
        this.retryExecutor = java.util.concurrent.Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "Disruptor-Retry");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 处理单个消息重试
     */
    public void handleRetry(DisruptorMessageEvent event) {
        Message message = event.getMessage();
        int currentRetryCount = event.getRetryCount();

        if (currentRetryCount < properties.getRetry().getMaxRetryCount()) {
            // 计算重试延迟
            long delay = calculateRetryDelay(currentRetryCount);

            // 调度重试
            retryExecutor.schedule(() -> {
                event.setRetryCount(currentRetryCount + 1);
                republishMessage(event);
            }, delay, java.util.concurrent.TimeUnit.MILLISECONDS);

            System.out.println("消息重试调度 - MessageId: " + message.getId() +
                    ", 重试次数: " + (currentRetryCount + 1) +
                    ", 延迟: " + delay + "ms");
        } else {
            handleFailed(event);
        }
    }

    /**
     * 处理消息失败
     */
    public void handleFailed(DisruptorMessageEvent event) {
        Message message = event.getMessage();
        sendToDeadLetterQueue(message);
    }

    /**
     * 处理异常
     */
    public void handleException(DisruptorMessageEvent event, Exception e) {
        handleRetry(event);
    }

    /**
     * 处理批量重试
     */
    public void handleBatchRetry(DisruptorBatchEvent event, BatchConsumeResult result) {
        java.util.List<String> failedIds = result.getFailedMessageIds();
        java.util.List<Message> failedMessages = new java.util.ArrayList<>();

        for (Message message : event.getMessages()) {
            if (failedIds.contains(message.getId())) {
                if (message.getRetryCount() < properties.getRetry().getMaxRetryCount()) {
                    message.setRetryCount(message.getRetryCount() + 1);
                    failedMessages.add(message);
                } else {
                    sendToDeadLetterQueue(message);
                }
            }
        }

        if (!failedMessages.isEmpty()) {
            republishBatchMessages(event.getTopic(), failedMessages);
        }
    }

    /**
     * 处理批量异常
     */
    public void handleBatchException(DisruptorBatchEvent event, Exception e) {
        // 将整个批次进行重试
        for (Message message : event.getMessages()) {
            if (message.getRetryCount() < properties.getRetry().getMaxRetryCount()) {
                message.setRetryCount(message.getRetryCount() + 1);
            } else {
                sendToDeadLetterQueue(message);
                return;
            }
        }

        republishBatchMessages(event.getTopic(), event.getMessages());
    }

    /**
     * 计算重试延迟
     */
    private long calculateRetryDelay(int retryCount) {
        long baseDelay = properties.getRetry().getRetryDelayMs();
        double multiplier = properties.getRetry().getRetryMultiplier();
        long maxDelay = properties.getRetry().getMaxRetryDelayMs();

        long delay = (long) (baseDelay * Math.pow(multiplier, retryCount));
        return Math.min(delay, maxDelay);
    }

    /**
     * 重新发布消息
     */
    private void republishMessage(DisruptorMessageEvent event) {
        // 这里需要访问到原始的RingBuffer来重新发布
        // 实际实现中需要通过回调或者其他方式来处理
        System.out.println("重新发布消息 - MessageId: " + event.getMessage().getId());
    }

    /**
     * 重新发布批量消息
     */
    private void republishBatchMessages(String topic, java.util.List<Message> messages) {
        System.out.println("重新发布批量消息 - Topic: " + topic + ", 数量: " + messages.size());
    }

    /**
     * 发送到死信队列
     */
    private void sendToDeadLetterQueue(Message message) {
        System.err.println("消息发送到死信队列 - MessageId: " + message.getId());
        // 实现死信队列逻辑
    }

    /**
     * 关闭重试处理器
     */
    public void shutdown() {
        retryExecutor.shutdown();
        try {
            if (!retryExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                retryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

