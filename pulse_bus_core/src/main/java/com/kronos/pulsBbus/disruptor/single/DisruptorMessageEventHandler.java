package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.disruptor.DisruptorRetryHandler;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:39
 * @desc
 */
public class DisruptorMessageEventHandler implements com.lmax.disruptor.EventHandler<DisruptorMessageEvent> {


    private final String                topic;
    private final MessageConsumer consumer;
    private final MessageQueueMetrics   metrics;
    private final DisruptorRetryHandler retryHandler;

    public DisruptorMessageEventHandler(String topic,
                                        MessageConsumer consumer,
                                        MessageQueueMetrics metrics,
                                        DisruptorRetryHandler retryHandler) {
        this.topic = topic;
        this.consumer = consumer;
        this.metrics = metrics;
        this.retryHandler = retryHandler;
    }

    @Override
    public void onEvent(DisruptorMessageEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.isProcessed() || event.getMessage() == null) {
            return;
        }

        long startTime = System.currentTimeMillis();
        Message message = event.getMessage();

        try {
            // 记录接收消息
            if (metrics != null) {
                metrics.recordReceivedMessage(topic);
            }

            // 执行消费
            ConsumeResult result = consumer.consume(message);

            // 记录消费延迟
            if (metrics != null) {
                metrics.recordConsumeLatency(topic, System.currentTimeMillis() - startTime);
            }

            // 处理消费结果
            handleConsumeResult(event, result);

        } catch (Exception e) {
            System.err.println("Disruptor消息消费异常 - Topic: " + topic +
                    ", MessageId: " + message.getId() + ", 错误: " + e.getMessage());

            if (metrics != null) {
                metrics.recordConsumeFailed(topic);
            }

            // 处理异常
            handleConsumeException(event, e);
        } finally {
            event.setProcessed(true);
        }
    }

    private void handleConsumeResult(DisruptorMessageEvent event, ConsumeResult result) {
        Message message = event.getMessage();

        switch (result) {
            case SUCCESS:
                if (metrics != null) {
                    metrics.recordConsumeSuccess(topic);
                }
                break;
            case RETRY:
                if (metrics != null) {
                    metrics.recordConsumeFailed(topic);
                }
                retryHandler.handleRetry(event);
                break;
            case SUSPEND:
                System.out.println("消息消费被暂停 - MessageId: " + message.getId());
                break;
            case FAILED:
                if (metrics != null) {
                    metrics.recordConsumeFailed(topic);
                }
                retryHandler.handleFailed(event);
                break;
        }
    }

    private void handleConsumeException(DisruptorMessageEvent event, Exception e) {
        retryHandler.handleException(event, e);
    }
}
