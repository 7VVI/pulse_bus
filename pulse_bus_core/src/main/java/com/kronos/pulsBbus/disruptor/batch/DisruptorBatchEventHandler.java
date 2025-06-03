package com.kronos.pulsBbus.disruptor.batch;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.batch.BatchConsumeResult;
import com.kronos.pulsBbus.core.batch.BatchMessageConsumer;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.disruptor.DisruptorRetryHandler;
import com.lmax.disruptor.EventHandler;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:45
 * @desc
 */
public class DisruptorBatchEventHandler implements EventHandler<DisruptorBatchEvent> {

    private final String                topic;
    private final BatchMessageConsumer  batchConsumer;
    private final MessageQueueMetrics   metrics;
    private final DisruptorRetryHandler retryHandler;

    public DisruptorBatchEventHandler(String topic,
                                      BatchMessageConsumer batchConsumer,
                                      MessageQueueMetrics metrics,
                                      DisruptorRetryHandler retryHandler) {
        this.topic = topic;
        this.batchConsumer = batchConsumer;
        this.metrics = metrics;
        this.retryHandler = retryHandler;
    }

    @Override
    public void onEvent(DisruptorBatchEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isEmpty() || event.isProcessed()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        java.util.List<Message> messages = event.getMessages();

        try {
            // 执行批量消费
            BatchConsumeResult result = batchConsumer.consumeBatch(messages);

            // 记录指标
            if (metrics != null) {
                metrics.recordBatchSent(topic, result.getSuccessCount(), result.getFailedCount());
                metrics.recordConsumeLatency(topic, System.currentTimeMillis() - startTime);
            }

            // 处理失败的消息
            handleBatchResult(event, result);

            System.out.println(String.format("Disruptor批量消费完成 - Topic: %s, 批次大小: %d, 成功: %d, 失败: %d",
                    topic, result.getTotalCount(), result.getSuccessCount(), result.getFailedCount()));

        } catch (Exception e) {
            System.err.println("Disruptor批量消费异常 - Topic: " + topic + ", 错误: " + e.getMessage());

            if (metrics != null) {
                for (Message message : messages) {
                    metrics.recordConsumeFailed(topic);
                }
            }

            // 处理批量异常
            retryHandler.handleBatchException(event, e);
        } finally {
            event.setProcessed(true);
        }
    }

    private void handleBatchResult(DisruptorBatchEvent event, BatchConsumeResult result) {
        if (result.getFailedCount() > 0) {
            retryHandler.handleBatchRetry(event, result);
        }
    }
}

