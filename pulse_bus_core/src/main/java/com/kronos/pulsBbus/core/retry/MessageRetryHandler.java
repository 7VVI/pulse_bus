package com.kronos.pulsBbus.core.retry;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.MessageQueueProperties;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyh
 * @Date 2025/5/28 19:34
 * @desc
 */
@Component
public class MessageRetryHandler {

    private final ScheduledExecutorService retryExecutor;
    private final MessageQueueMetrics      metrics;
    private final MessageQueueProperties   properties;

    public MessageRetryHandler(MessageQueueMetrics metrics, MessageQueueProperties properties) {
        this.metrics = metrics;
        this.properties = properties;
        this.retryExecutor = Executors.newScheduledThreadPool(2,
                r -> new Thread(r, "message-retry-"));
    }

    public void handleRetry(Message message, MessageConsumer consumer, ConsumeResult result) {
        if (result != ConsumeResult.RETRY) {
            return;
        }

        int currentRetryCount = message.getRetryCount();
        MessageQueueProperties.RetryPolicy retryPolicy = properties.getRetryPolicy();

        if (currentRetryCount >= retryPolicy.getMaxRetryCount()) {
            // 超过最大重试次数，记录失败
            metrics.recordConsumeFailed(message.getTopic());
            return;
        }

        // 计算延迟时间（指数退避）
        long delay = calculateRetryDelay(currentRetryCount, retryPolicy);

        // 增加重试次数
        message.setRetryCount(currentRetryCount + 1);

        // 调度重试
        retryExecutor.schedule(() -> {
            try {
                ConsumeResult retryResult = consumer.consume(message);
                if (retryResult == ConsumeResult.RETRY) {
                    handleRetry(message, consumer, retryResult);
                } else if (retryResult == ConsumeResult.SUCCESS) {
                    metrics.recordConsumeSuccess(message.getTopic());
                } else {
                    metrics.recordConsumeFailed(message.getTopic());
                }
            } catch (Exception e) {
                metrics.recordConsumeFailed(message.getTopic());
            }
        }, delay, TimeUnit.MILLISECONDS);

        metrics.recordConsumeRetry(message.getTopic());
    }

    private long calculateRetryDelay(int retryCount, MessageQueueProperties.RetryPolicy policy) {
        long delay = (long) (policy.getRetryDelayMs() * Math.pow(policy.getBackoffMultiplier(), retryCount));
        return Math.min(delay, policy.getMaxRetryDelayMs());
    }

    @PreDestroy
    public void shutdown() {
        if (retryExecutor != null) {
            retryExecutor.shutdown();
            try {
                if (!retryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    retryExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                retryExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}

