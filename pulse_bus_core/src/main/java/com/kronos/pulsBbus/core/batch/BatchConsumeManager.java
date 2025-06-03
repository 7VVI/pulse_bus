package com.kronos.pulsBbus.core.batch;

import com.kronos.pulsBbus.core.Message;

import java.util.List;
import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:56
 * @desc
 */
@org.springframework.stereotype.Component
public class BatchConsumeManager {

    private final Map<String, BatchMessageConsumer>             batchConsumers = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, BatchConsumeConfig>               batchConfigs   = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, MessageBuffer>                    messageBuffers = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ScheduledExecutorService scheduler      =
            java.util.concurrent.Executors.newScheduledThreadPool(5);

    /**
     * 注册批量消费者
     */
    public void registerBatchConsumer(String topic, BatchMessageConsumer consumer, BatchConsumeConfig config) {
        batchConsumers.put(topic, consumer);
        batchConfigs.put(topic, config);
        messageBuffers.put(topic, new MessageBuffer(config));

        // 启动定时批量消费任务
        startBatchConsumeTask(topic);
    }

    /**
     * 添加消息到批量缓冲区
     */
    public void addMessage(String topic, Message message) {
        MessageBuffer buffer = messageBuffers.get(topic);
        if (buffer != null) {
            buffer.addMessage(message);

            // 检查是否达到批次大小，立即触发消费
            BatchConsumeConfig config = batchConfigs.get(topic);
            if (buffer.size() >= config.getBatchSize()) {
                triggerBatchConsume(topic);
            }
        }
    }

    /**
     * 启动批量消费定时任务
     */
    private void startBatchConsumeTask(String topic) {
        BatchConsumeConfig config = batchConfigs.get(topic);

        scheduler.scheduleAtFixedRate(() -> {
            triggerBatchConsume(topic);
        }, config.getBatchTimeoutMs(), config.getBatchTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * 触发批量消费
     */
    private void triggerBatchConsume(String topic) {
        MessageBuffer buffer = messageBuffers.get(topic);
        BatchMessageConsumer consumer = batchConsumers.get(topic);
        BatchConsumeConfig config = batchConfigs.get(topic);

        if (buffer != null && consumer != null && !buffer.isEmpty()) {
            List<Message> messages = buffer.drainMessages(config.getBatchSize());

            if (!messages.isEmpty()) {
                try {
                    BatchConsumeResult result = consumer.consumeBatch(messages);
                    handleBatchConsumeResult(topic, result, messages);
                } catch (Exception e) {
                    handleBatchConsumeException(topic, messages, e);
                }
            }
        }
    }

    /**
     * 处理批量消费结果
     */
    private void handleBatchConsumeResult(String topic, BatchConsumeResult result, List<Message> messages) {
        System.out.println(String.format("批量消费完成 - Topic: %s, 总数: %d, 成功: %d, 失败: %d",
                topic, result.getTotalCount(), result.getSuccessCount(), result.getFailedCount()));

        // 处理失败的消息
        if (result.getFailedCount() > 0) {
            handleFailedMessages(topic, result.getFailedMessageIds(), messages);
        }
    }

    /**
     * 处理批量消费异常
     */
    private void handleBatchConsumeException(String topic, List<Message> messages, Exception e) {
        System.err.println("批量消费异常 - Topic: " + topic + ", 错误: " + e.getMessage());

        // 将消息重新放回缓冲区进行重试
        MessageBuffer buffer = messageBuffers.get(topic);
        if (buffer != null) {
            for (Message message : messages) {
                if (message.getRetryCount() < batchConfigs.get(topic).getMaxRetryCount()) {
                    message.setRetryCount(message.getRetryCount() + 1);
                    buffer.addMessage(message);
                }
            }
        }
    }

    /**
     * 处理失败的消息
     */
    private void handleFailedMessages(String topic, List<String> failedMessageIds, List<Message> messages) {
        MessageBuffer buffer = messageBuffers.get(topic);
        BatchConsumeConfig config = batchConfigs.get(topic);

        if (buffer != null) {
            for (Message message : messages) {
                if (failedMessageIds.contains(message.getId())) {
                    if (message.getRetryCount() < config.getMaxRetryCount()) {
                        message.setRetryCount(message.getRetryCount() + 1);
                        buffer.addMessage(message);
                    } else {
                        // 达到最大重试次数，发送到死信队列
                        sendToDeadLetterQueue(topic, message);
                    }
                }
            }
        }
    }

    /**
     * 发送到死信队列
     */
    private void sendToDeadLetterQueue(String topic, Message message) {
        System.err.println("消息发送到死信队列 - Topic: " + topic + ", MessageId: " + message.getId());
        // 实现死信队列逻辑
    }
}
