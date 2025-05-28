package com.kronos.pulsBbus.core.monitor;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.single.SendResult;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:12
 * @desc
 */
public class MetricsWrappedTemplate implements MessageQueueTemplate {

    private final MessageQueueTemplate delegate;
    private final MessageQueueMetrics  metrics;

    public MetricsWrappedTemplate(MessageQueueTemplate delegate, MessageQueueMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    public SendResult send(String topic, Object message) {
        long startTime = System.currentTimeMillis();
        try {
            SendResult result = delegate.send(topic, message);

            // 记录发送指标
            if (result.isSuccess()) {
                metrics.recordSentMessage(topic);
                metrics.recordSendLatency(topic, System.currentTimeMillis() - startTime);
            } else {
                metrics.recordFailedMessage(topic);
            }

            return result;
        } catch (Exception e) {
            metrics.recordFailedMessage(topic);
            throw e;
        }
    }

    @Override
    public java.util.concurrent.CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        long startTime = System.currentTimeMillis();

        return delegate.sendAsync(topic, message)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        metrics.recordFailedMessage(topic);
                    } else if (result.isSuccess()) {
                        metrics.recordSentMessage(topic);
                        metrics.recordSendLatency(topic, System.currentTimeMillis() - startTime);
                    } else {
                        metrics.recordFailedMessage(topic);
                    }
                });
    }

    @Override
    public BatchSendResult sendBatch(String topic, java.util.List<Object> messages) {
        long startTime = System.currentTimeMillis();

        BatchSendResult result = delegate.sendBatch(topic, messages);

        // 记录批量发送指标
        metrics.recordBatchSent(topic, result.getSuccessCount(), result.getFailedCount());
        metrics.recordSendLatency(topic, System.currentTimeMillis() - startTime);

        return result;
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        // 包装消费者，添加监控
        MessageConsumer wrappedConsumer = message -> {
            long startTime = System.currentTimeMillis();
            try {
                ConsumeResult result = consumer.consume(message);

                // 记录消费指标
                metrics.recordReceivedMessage(topic);
                metrics.recordConsumeLatency(topic, System.currentTimeMillis() - startTime);

                if (result == ConsumeResult.SUCCESS) {
                    metrics.recordConsumeSuccess(topic);
                } else {
                    metrics.recordConsumeFailed(topic);
                }

                return result;
            } catch (Exception e) {
                metrics.recordConsumeFailed(topic);
                throw e;
            }
        };

        delegate.subscribe(topic, wrappedConsumer);
    }

    // 委托其他方法
    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        return delegate.sendDelay(topic, message, delayMs);
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        return delegate.sendTransaction(topic, message, callback);
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        delegate.unsubscribe(topic, consumerGroup);
    }
}
