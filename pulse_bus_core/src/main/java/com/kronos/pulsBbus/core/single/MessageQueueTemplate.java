package com.kronos.pulsBbus.core.single;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:25
 * @desc
 */

import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;

import java.util.concurrent.CompletableFuture;

/**
 * 统一消息接口 - 屏蔽不同MQ中间件差异
 */
public interface MessageQueueTemplate {

    /**
     * 同步发送消息
     */
    SendResult send(String topic, Object message);

    /**
     * 异步发送消息
     */
    CompletableFuture<SendResult> sendAsync(String topic, Object message);

    /**
     * 批量发送消息
     */
    BatchSendResult sendBatch(String topic, java.util.List<Object> messages);

    /**
     * 发送延迟消息
     */
    SendResult sendDelay(String topic, Object message, long delayMs);

    /**
     * 发送事务消息
     */
    SendResult sendTransaction(String topic, Object message, TransactionCallback callback);

    /**
     * 订阅消息
     */
    void subscribe(String topic, MessageConsumer consumer);

    /**
     * 取消订阅
     */
    void unsubscribe(String topic, String consumerGroup);
}
