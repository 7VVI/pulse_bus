package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangyh
 * @Date 2025/5/28 19:40
 * @desc
 */
public class DisruptorTemplate implements MessageQueueTemplate {

    private final DisruptorProvider provider;

    public DisruptorTemplate(DisruptorProvider provider) {
        this.provider = provider;
    }

    @Override
    public SendResult send(String topic, Object message) {
        Message msg = createMessage(topic, message);
        return provider.sendMessage(topic, msg);
    }

    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return CompletableFuture.supplyAsync(() -> send(topic, message));
    }

    @Override
    public BatchSendResult sendBatch(String topic, List<Object> messages) {
        BatchSendResult result = new BatchSendResult(100);

        for (Object message : messages) {
            SendResult sendResult = send(topic, message);
            if (sendResult.isSuccess()) {
                result.addSuccess(sendResult.getMessageId());
            } else {
                result.addFailure(sendResult.getErrorMessage());
            }
        }

        return result;
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        // Disruptor本身不支持延迟消息，可以通过调度器实现
        Message msg = createMessage(topic, message);
        msg.setDelayTime(delayMs);
        msg.setMessageType(Message.MessageType.DELAY);

        if (delayMs <= 0) {
            return provider.sendMessage(topic, msg);
        }

        // 使用调度器延迟发送
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> provider.sendMessage(topic, msg), delayMs, TimeUnit.MILLISECONDS);
        scheduler.shutdown();

        return new SendResult(true, msg.getId());
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        // Disruptor不支持事务，这里提供简单的回调机制
        try {
            if (callback.doInTransaction()) {
                return send(topic, message);
            } else {
                callback.rollback();
                return new SendResult(false, null, "Transaction callback returned false");
            }
        } catch (Exception e) {
            callback.rollback();
            return new SendResult(false, null, "Transaction failed: " + e.getMessage());
        }
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        provider.subscribeMessage(topic, consumer);
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        // Disruptor没有消费者组概念，这里简化处理
        Set<MessageConsumer> consumers = provider.getTopicConsumers(topic);
        consumers.clear();
    }

    private Message createMessage(String topic, Object payload) {
        Message message = new Message(topic, payload);
        message.setTimestamp(java.time.LocalDateTime.now());
        return message;
    }
}
