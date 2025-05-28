package com.kronos.pulsBbus.core.batch;

import com.kronos.pulsBbus.core.*;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;

import java.util.List;
import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:57
 * @desc 支持批量消费的模板实现
 */
public class BatchSupportMessageTemplate implements BatchMessageQueueTemplate {

    private final MessageQueueTemplate delegate;
    private final BatchConsumeManager  batchConsumeManager;

    public BatchSupportMessageTemplate(MessageQueueTemplate delegate, BatchConsumeManager batchConsumeManager) {
        this.delegate = delegate;
        this.batchConsumeManager = batchConsumeManager;
    }

    @Override
    public void subscribeBatch(String topic, BatchMessageConsumer consumer, BatchConsumeConfig config) {
        // 注册批量消费者
        batchConsumeManager.registerBatchConsumer(topic, consumer, config);

        // 订阅单个消息，转发到批量消费管理器
        delegate.subscribe(topic, message -> {
            batchConsumeManager.addMessage(topic, message);
            return ConsumeResult.SUCCESS; // 单个消息总是返回成功，由批量消费处理
        });
    }

    @Override
    public void unsubscribeBatch(String topic) {
        delegate.unsubscribe(topic, null);
    }

    @Override
    public Map<String, Object> getBatchConsumeStats(String topic) {
        Map<String, Object> stats = new java.util.HashMap<>();
        // 实现统计信息收集
        return stats;
    }

    // 委托其他方法到原始模板
    @Override
    public SendResult send(String topic, Object message) {
        return delegate.send(topic, message);
    }

    @Override
    public java.util.concurrent.CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return delegate.sendAsync(topic, message);
    }

    @Override
    public BatchSendResult sendBatch(String topic, List<Object> messages) {
        return delegate.sendBatch(topic, messages);
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        return delegate.sendDelay(topic, message, delayMs);
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        return delegate.sendTransaction(topic, message, callback);
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        delegate.subscribe(topic, consumer);
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        delegate.unsubscribe(topic, consumerGroup);
    }
}

