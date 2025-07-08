package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.delay.DelayMessageProcessor;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.retry.RetryMessage;
import com.kronos.pulsBbus.core.retry.RetryMessageProcessor;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:47
 * @desc Redis消息队列模板适配器
 */
public class RedisTemplateAdapter implements MessageQueueTemplate {

    private static final Logger log = LoggerFactory.getLogger(RedisTemplateAdapter.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisProperties properties;
    private final MessageQueueMetrics metrics;
    private final ExecutorService executorService;
    private final RedisMessageListenerContainer listenerContainer;
    private final ConcurrentHashMap<String, MessageConsumer> consumers;
    private final DelayMessageProcessor delayMessageProcessor;
    private final RetryMessageProcessor retryMessageProcessor;

    public RedisTemplateAdapter(RedisTemplate<String, Object> redisTemplate, 
                               RedisProperties properties, 
                               MessageQueueMetrics metrics,
                               DelayMessageProcessor delayMessageProcessor,
                               RetryMessageProcessor retryMessageProcessor) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metrics = metrics;
        this.executorService = Executors.newFixedThreadPool(10);
        this.consumers = new ConcurrentHashMap<>();
        this.delayMessageProcessor = delayMessageProcessor;
        this.retryMessageProcessor = retryMessageProcessor;
        
        // 初始化消息监听容器
        this.listenerContainer = new RedisMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(redisTemplate.getConnectionFactory());
        this.listenerContainer.start();
        
        // 启动延迟消息处理器
        if (properties.getDelay().isEnabled() && delayMessageProcessor != null) {
            delayMessageProcessor.start();
        }
        
        // 启动重试消息处理器
        if (properties.getRetry().getMaxRetryCount() > 0 && retryMessageProcessor != null) {
            retryMessageProcessor.start();
        }
    }

    @Override
    public SendResult send(String topic, Object message) {
        long startTime = System.currentTimeMillis();
        String messageId = UUID.randomUUID().toString();
        
        try {
            String key = buildTopicKey(topic);
            
            // 创建消息包装对象
            RedisMessage redisMessage = new RedisMessage(messageId, message, System.currentTimeMillis());
            
            // 发送到Redis列表
            redisTemplate.opsForList().rightPush(key, redisMessage);
            
            // 设置TTL
            if (properties.getMessage().getDefaultTtl() > 0) {
                redisTemplate.expire(key, properties.getMessage().getDefaultTtl(), TimeUnit.MILLISECONDS);
            }
            
            // 记录指标
            if (metrics != null) {
                metrics.recordSentMessage(topic);
            }
            
            log.debug("消息发送成功 - Topic: {}, MessageId: {}", topic, messageId);
            return new SendResult(true, messageId);
            
        } catch (Exception e) {
            // 记录指标
            if (metrics != null) {
                metrics.recordFailedMessage(topic);
            }
            
            log.error("消息发送失败 - Topic: {}, MessageId: {}", topic, messageId, e);
            SendResult result = new SendResult(false, messageId);
            result.setErrorMessage(e.getMessage());
            return result;
        }
    }

    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return send(topic, message);
            } catch (Exception e) {
                log.error("异步发送消息失败 - Topic: {}", topic, e);
                SendResult result = new SendResult(false, null);
                result.setErrorMessage(e.getMessage());
                return result;
            }
        }, executorService);
    }

    @Override
    public BatchSendResult sendBatch(String topic, List<Object> messages) {
        BatchSendResult batchResult = new BatchSendResult(messages.size());
        
        if (messages.isEmpty()) {
            return batchResult;
        }
        
        try {
            String key = buildTopicKey(topic);
            
            // 创建Redis消息列表
            Object[] redisMessages = messages.stream()
                    .map(msg -> new RedisMessage(UUID.randomUUID().toString(), msg, System.currentTimeMillis()))
                    .toArray();
            
            // 批量发送
            redisTemplate.opsForList().rightPushAll(key, redisMessages);
            
            // 设置TTL
            if (properties.getMessage().getDefaultTtl() > 0) {
                redisTemplate.expire(key, properties.getMessage().getDefaultTtl(), TimeUnit.MILLISECONDS);
            }
            
            batchResult.setSuccessCount(messages.size());
            batchResult.setFailedCount(0);
            
            // 记录指标
            if (metrics != null) {
                metrics.recordBatchSent(topic, messages.size(), batchResult.getFailedCount());
            }
            
            log.debug("批量消息发送成功 - Topic: {}, Count: {}", topic, messages.size());
            
        } catch (Exception e) {
            batchResult.setSuccessCount(0);
            batchResult.setFailedCount(messages.size());
            
            // 记录指标
            if (metrics != null) {
                metrics.recordBatchSent(topic, 0, messages.size());
            }
            
            log.error("批量消息发送失败 - Topic: {}, Count: {}", topic, messages.size(), e);
        }
        
        return batchResult;
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        if (!properties.getDelay().isEnabled()) {
            throw new UnsupportedOperationException("延迟消息功能未启用");
        }
        
        if (delayMessageProcessor == null) {
            throw new UnsupportedOperationException("延迟消息处理器未配置");
        }
        
        String messageId = UUID.randomUUID().toString();
        
        try {
            // 使用延迟消息处理器发送
            String resultMessageId = delayMessageProcessor.sendDelayMessage(topic, message, delayMs);
            messageId = resultMessageId; // 使用处理器返回的消息ID
            
            // 记录指标
            if (metrics != null) {
                metrics.recordDelayMessageSent(topic);
            }
            
            log.debug("延迟消息发送成功 - Topic: {}, MessageId: {}, DelayMs: {}", topic, messageId, delayMs);
            return new SendResult(true, messageId);
            
        } catch (Exception e) {
            // 记录指标
            if (metrics != null) {
                metrics.recordDelayMessageFailed(topic);
            }
            
            log.error("延迟消息发送失败 - Topic: {}, MessageId: {}, DelayMs: {}", topic, messageId, delayMs, e);
            SendResult result = new SendResult(false, messageId);
            result.setErrorMessage(e.getMessage());
            return result;
        }
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        String messageId = UUID.randomUUID().toString();
        
        try {
            // Redis事务执行
            List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {
                @Override
                public List<Object> execute(RedisOperations operations) throws DataAccessException {
                    operations.multi();
                    
                    try {
                        // 执行回调
                        if (callback != null) {
                            callback.doInTransaction();
                        }
                        
                        // 发送消息
                        String key = buildTopicKey(topic);
                        RedisMessage redisMessage = new RedisMessage(messageId, message, System.currentTimeMillis());
                        
                        // 使用RedisTemplate的高级操作在事务中发送消息
                        operations.opsForList().rightPush(key, redisMessage);
                        
                        return operations.exec();
                        
                    } catch (Exception e) {
                        operations.discard();
                        throw new RuntimeException("事务执行失败", e);
                    }
                }
            });
            
            if (results != null && !results.isEmpty()) {
                log.debug("事务消息发送成功 - Topic: {}, MessageId: {}", topic, messageId);
                return new SendResult(true, messageId);
            } else {
                log.warn("事务消息发送失败 - Topic: {}, MessageId: {}", topic, messageId);
                SendResult result = new SendResult(false, messageId);
                result.setErrorMessage("事务执行失败");
                return result;
            }
            
        } catch (Exception e) {
            log.error("事务消息发送异常 - Topic: {}, MessageId: {}", topic, messageId, e);
            SendResult result = new SendResult(false, messageId);
            result.setErrorMessage(e.getMessage());
            return result;
        }
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        try {
            String key = buildTopicKey(topic);
            
            // 创建消息监听器
            MessageListener listener = (message, pattern) -> {
                Object messageObj = null;
                RedisMessage redisMessage = null;
                try {
                    // 从Redis List中获取消息
                    messageObj = redisTemplate.opsForList().leftPop(key);
                    if (messageObj != null) {
                        // 处理不同类型的消息对象
                        if (messageObj instanceof RedisMessage) {
                            redisMessage = (RedisMessage) messageObj;
                        } else if (messageObj instanceof byte[]) {
                            // 反序列化字节数组
                            try {
                                redisMessage = (RedisMessage) redisTemplate.getValueSerializer().deserialize((byte[]) messageObj);
                            } catch (Exception deserializeEx) {
                                log.error("消息反序列化失败 - Topic: {}", topic, deserializeEx);
                                return;
                            }
                        } else {
                            // 尝试直接转换
                            try {
                                redisMessage = (RedisMessage) messageObj;
                            } catch (ClassCastException castEx) {
                                log.error("消息类型转换失败 - Topic: {}, MessageType: {}", topic, messageObj.getClass().getName());
                                return;
                            }
                        }
                        
                        if (redisMessage != null) {
                            // 创建Message对象传递给消费者
                            Message realMessage = new Message(topic, redisMessage.getPayload());
                            realMessage.setId(redisMessage.getMessageId());
                            realMessage.setRetryCount(redisMessage.getRetryCount());
                            realMessage.setTimestamp(java.time.LocalDateTime.ofInstant(
                                java.time.Instant.ofEpochMilli(redisMessage.getTimestamp()),
                                java.time.ZoneId.systemDefault()
                            ));
                            
                            ConsumeResult result = consumer.consume(realMessage);
                            
                            // 记录消费指标
                            if (metrics != null) {
                                if (result == ConsumeResult.SUCCESS) {
                                    metrics.recordConsumeSuccess(topic);
                                } else {
                                    metrics.recordConsumeFailed(topic);
                                }
                            }
                            
                            log.debug("消息消费成功 - Topic: {}, MessageId: {}, Result: {}", topic, redisMessage.getMessageId(), result);
                        }
                    }
                } catch (Exception e) {
                    // 记录消费失败指标
                    if (metrics != null) {
                        metrics.recordConsumeFailed(topic);
                    }
                    
                    log.error("消息消费失败 - Topic: {}", topic, e);
                    
                    // 根据配置决定是否重试
                    if (properties.getRetry().getMaxRetryCount() > 0 && redisMessage != null && retryMessageProcessor != null) {
                        try {
                            // 创建重试消息
                            RetryMessage retryMessage = new RetryMessage(
                                    redisMessage.getMessageId(),
                                    redisMessage.getPayload(),
                                    topic,
                                    e.getMessage(),
                                    1 // 初始重试次数
                            );
                            
                            // 设置额外属性
                            retryMessage.setMaxRetryCount(properties.getRetry().getMaxRetryCount());
                            retryMessage.setNextRetryTime(System.currentTimeMillis() + properties.getRetry().getRetryDelayMs());
                            
                            // 使用重试消息处理器处理
                            retryMessageProcessor.handleRetry(topic, retryMessage.getOriginalMessage(), consumer, e);
                            
                            // 记录指标
                            if (metrics != null) {
                                metrics.recordRetryMessageSent(topic);
                            }
                            
                            log.debug("消息已添加到重试队列 - Topic: {}, MessageId: {}", topic, redisMessage.getMessageId());
                        } catch (Exception retryEx) {
                            log.error("添加重试消息失败 - Topic: {}, MessageId: {}", topic, redisMessage.getMessageId(), retryEx);
                        }
                    }
                }
            };
            
            // 添加监听器到容器
            listenerContainer.addMessageListener(listener, new PatternTopic(key));
            
            log.info("订阅成功 - Topic: {}", topic);
            
        } catch (Exception e) {
            log.error("订阅失败 - Topic: {}", topic, e);
            throw new RuntimeException("订阅失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        // 取消订阅
    }
    
    /**
     * 销毁资源
     */
    public void destroy() {
        try {
            // 停止延迟消息处理器
            if (delayMessageProcessor != null) {
                delayMessageProcessor.stop();
            }
            
            // 停止重试消息处理器
            if (retryMessageProcessor != null) {
                retryMessageProcessor.stop();
            }
            
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
            }
            
            if (listenerContainer != null && listenerContainer.isRunning()) {
                listenerContainer.stop();
            }
            
            log.info("RedisTemplateAdapter资源已销毁");
        } catch (Exception e) {
            log.error("销毁RedisTemplateAdapter资源时发生错误", e);
        }
    }

    // ==================== 辅助方法 ====================
    
    /**
     * 构建Topic键名
     */
    private String buildTopicKey(String topic) {
        return properties.getMessage().getKeyPrefix() + topic;
    }
    

    

}

