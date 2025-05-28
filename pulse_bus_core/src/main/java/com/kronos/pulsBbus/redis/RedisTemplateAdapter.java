package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.*;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:47
 * @desc
 */
public class RedisTemplateAdapter implements MessageQueueTemplate {

    private final org.springframework.data.redis.core.RedisTemplate<String, Object> redisTemplate;
    private final java.util.concurrent.ExecutorService executorService;

    public RedisTemplateAdapter(org.springframework.data.redis.core.RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.executorService = java.util.concurrent.Executors.newFixedThreadPool(10);
    }

    @Override
    public SendResult send(String topic, Object message) {
        try {
            String messageId = java.util.UUID.randomUUID().toString();
            redisTemplate.opsForList().rightPush(topic, message);
            return new SendResult(true, messageId);
        } catch (Exception e) {
            SendResult result = new SendResult(false, null);
            result.setErrorMessage(e.getMessage());
            return result;
        }
    }

    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return CompletableFuture.supplyAsync(() -> send(topic, message), executorService);
    }

    @Override
    public BatchSendResult sendBatch(String topic, java.util.List<Object> messages) {
        BatchSendResult batchResult = new BatchSendResult(messages.size());
        try {
            redisTemplate.opsForList().rightPushAll(topic, messages.toArray());
            batchResult.setSuccessCount(messages.size());
            batchResult.setFailedCount(0);
        } catch (Exception e) {
            batchResult.setSuccessCount(0);
            batchResult.setFailedCount(messages.size());
        }
        return batchResult;
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        try {
            String messageId = java.util.UUID.randomUUID().toString();
            long score = System.currentTimeMillis() + delayMs;
            redisTemplate.opsForZSet().add(topic + ":delay", message, score);
            return new SendResult(true, messageId);
        } catch (Exception e) {
            SendResult result = new SendResult(false, null);
            result.setErrorMessage(e.getMessage());
            return result;
        }
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        // Redis事务实现
        return send(topic, message);
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        // Redis订阅实现
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        // 取消订阅
    }
}

