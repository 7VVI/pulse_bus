package com.kronos.pulsBbus.disruptor.batch;

import com.kronos.pulsBbus.core.*;
import com.kronos.pulsBbus.core.batch.BatchConsumeResult;
import com.kronos.pulsBbus.core.batch.BatchMessageConsumer;
import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.batch.BatchConsumeConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:27
 * @desc
 */
public class BatchConsumeEventHandler implements com.lmax.disruptor.EventHandler<BatchConsumeEvent> {

    private final BatchMessageConsumer batchConsumer;
    private final BatchConsumeConfig   config;
    private final MessageQueueMetrics  metrics;

    public BatchConsumeEventHandler(BatchMessageConsumer batchConsumer,
                                    BatchConsumeConfig config,
                                    MessageQueueMetrics metrics) {
        this.batchConsumer = batchConsumer;
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public void onEvent(BatchConsumeEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEmpty() || event.isProcessed()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String topic = event.getTopic();

        try {
            // 执行批量消费
            BatchConsumeResult result = batchConsumer.consumeBatch(event.getMessages());

            // 记录指标
            if (metrics != null) {
                metrics.recordBatchSent(topic, result.getSuccessCount(), result.getFailedCount());
                metrics.recordConsumeLatency(topic, System.currentTimeMillis() - startTime);

                // 记录每个消息的消费结果
                for (ConsumeResult consumeResult : result.getResults()) {
                    if (consumeResult == ConsumeResult.SUCCESS) {
                        metrics.recordConsumeSuccess(topic);
                    } else {
                        metrics.recordConsumeFailed(topic);
                    }
                }
            }

            // 处理失败的消息
            handleFailedMessages(event, result);

            System.out.println(String.format("Disruptor批量消费完成 - Topic: %s, 批次大小: %d, 成功: %d, 失败: %d",
                    topic, result.getTotalCount(), result.getSuccessCount(), result.getFailedCount()));

        } catch (Exception e) {
            System.err.println("Disruptor批量消费异常 - Topic: " + topic + ", 错误: " + e.getMessage());

            // 记录失败指标
            if (metrics != null) {
                for (Message message : event.getMessages()) {
                    metrics.recordConsumeFailed(topic);
                }
            }

            // 处理异常情况下的重试
            handleConsumeException(event, e);
        } finally {
            event.setProcessed(true);
        }
    }

    /**
     * 处理失败的消息
     */
    private void handleFailedMessages(BatchConsumeEvent event, BatchConsumeResult result) {
        if (result.getFailedCount() > 0) {
            List<Message> failedMessages = new ArrayList<>();
            List<String> failedIds = result.getFailedMessageIds();

            for (Message message : event.getMessages()) {
                if (failedIds.contains(message.getId())) {
                    if (message.getRetryCount() < config.getMaxRetryCount()) {
                        message.setRetryCount(message.getRetryCount() + 1);
                        failedMessages.add(message);
                    } else {
                        // 发送到死信队列
                        sendToDeadLetterQueue(message);
                    }
                }
            }

            // 重新发布失败的消息进行重试
            if (!failedMessages.isEmpty()) {
                republishFailedMessages(failedMessages);
            }
        }
    }

    /**
     * 处理消费异常
     */
    private void handleConsumeException(BatchConsumeEvent event, Exception e) {
        List<Message> retryMessages = new ArrayList<>();

        for (Message message : event.getMessages()) {
            if (message.getRetryCount() < config.getMaxRetryCount()) {
                message.setRetryCount(message.getRetryCount() + 1);
                retryMessages.add(message);
            } else {
                sendToDeadLetterQueue(message);
            }
        }

        if (!retryMessages.isEmpty()) {
            republishFailedMessages(retryMessages);
        }
    }

    /**
     * 重新发布失败的消息
     */
    private void republishFailedMessages(List<Message> messages) {
        // 这里需要访问到Disruptor的RingBuffer来重新发布消息
        // 实际实现中需要通过回调或者其他方式来处理
        System.out.println("重新发布失败消息数量: " + messages.size());
    }

    /**
     * 发送到死信队列
     */
    private void sendToDeadLetterQueue(Message message) {
        System.err.println("消息发送到死信队列 - MessageId: " + message.getId());
        // 实现死信队列逻辑
    }
}
