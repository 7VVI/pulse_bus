package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
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

import java.io.Serializable;
import java.util.List;
import java.util.Set;
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
    private final ScheduledExecutorService delayExecutor;
    private final RedisMessageListenerContainer listenerContainer;
    private final ConcurrentHashMap<String, MessageConsumer> consumers;

    public RedisTemplateAdapter(RedisTemplate<String, Object> redisTemplate, 
                               RedisProperties properties, 
                               MessageQueueMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metrics = metrics;
        this.executorService = Executors.newFixedThreadPool(10);
        this.delayExecutor = Executors.newScheduledThreadPool(properties.getDelay().getWorkerThreads());
        this.consumers = new ConcurrentHashMap<>();
        
        // 初始化消息监听容器
        this.listenerContainer = new RedisMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(redisTemplate.getConnectionFactory());
        this.listenerContainer.start();
        
        // 启动延迟消息处理器
        if (properties.getDelay().isEnabled()) {
            startDelayMessageProcessor();
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
        
        String messageId = UUID.randomUUID().toString();
        
        try {
            String delayKey = buildDelayKey(topic);
            long executeTime = System.currentTimeMillis() + delayMs;
            
            // 创建延迟消息
            RedisDelayMessage delayMessage = new RedisDelayMessage(
                    messageId, message, topic, executeTime, System.currentTimeMillis()
            );
            
            // 添加到延迟队列（使用ZSet，score为执行时间）
            redisTemplate.opsForZSet().add(delayKey, delayMessage, executeTime);
            
            log.debug("延迟消息发送成功 - Topic: {}, MessageId: {}, DelayMs: {}", topic, messageId, delayMs);
            return new SendResult(true, messageId);
            
        } catch (Exception e) {
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
                    if (properties.getRetry().getMaxRetryCount() > 0 && redisMessage != null) {
                        handleRetry(topic, redisMessage, e);
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
            if (delayExecutor != null && !delayExecutor.isShutdown()) {
                delayExecutor.shutdown();
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
     * 启动延迟消息处理器
     */
    private void startDelayMessageProcessor() {
        delayExecutor.scheduleWithFixedDelay(
            this::processDelayMessages,
            0,
            properties.getDelay().getScanIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        log.info("延迟消息处理器已启动，扫描间隔: {}ms", properties.getDelay().getScanIntervalMs());
    }
    
    /**
     * 构建Topic键名
     */
    private String buildTopicKey(String topic) {
        return properties.getMessage().getKeyPrefix() + topic;
    }
    
    /**
     * 构建延迟消息键名
     */
    private String buildDelayKey(String topic) {
        return buildTopicKey(topic) + ":delay";
    }
    
    /**
     * 处理消息重试
     */
    private void handleRetry(String topic, Object message, Exception error) {
        if (properties.getRetry().getMaxRetryCount() <= 0) {
            return;
        }
        
        try {
            String retryKey = buildTopicKey(topic) + ":retry";
            
            // 检查是否已经是重试消息
            int currentRetryCount = 1;
            Object originalMessage = message;
            
            if (message instanceof RetryMessage) {
                RetryMessage existingRetry = (RetryMessage) message;
                currentRetryCount = existingRetry.getRetryCount() + 1;
                originalMessage = existingRetry.getOriginalMessage();
                
                // 检查是否超过最大重试次数
                if (currentRetryCount > properties.getRetry().getMaxRetryCount()) {
                    log.warn("消息重试次数已达上限，丢弃消息 - Topic: {}, RetryCount: {}", topic, currentRetryCount - 1);
                    return;
                }
            }
            
            RetryMessage retryMessage = new RetryMessage(originalMessage, error.getMessage(), currentRetryCount, System.currentTimeMillis());
            
            // 计算下次重试时间（指数退避）
            long retryDelay = properties.getRetry().getRetryDelayMs() * (long) Math.pow(2, currentRetryCount - 1);
            long nextRetryTime = System.currentTimeMillis() + retryDelay;
            
            // 添加到延迟重试队列
            redisTemplate.opsForZSet().add(retryKey + ":delay", retryMessage, nextRetryTime);
            
            log.debug("消息已添加到重试队列 - Topic: {}, RetryCount: {}, NextRetryTime: {}", 
                    topic, currentRetryCount, nextRetryTime);
            
        } catch (Exception e) {
            log.error("添加重试消息失败 - Topic: {}", topic, e);
        }
    }
    
    /**
     * 延迟消息处理器
     */
    private void processDelayMessages() {
        try {
            long currentTime = System.currentTimeMillis();
            
            // 处理延迟消息
            if (properties.getDelay().isEnabled()) {
                processDelayedMessages(currentTime);
            }
            
            // 处理重试消息
            if (properties.getRetry().getMaxRetryCount() > 0) {
                processRetryMessages(currentTime);
            }
            
        } catch (Exception e) {
            log.error("延迟消息处理器执行失败", e);
        }
    }
    
    /**
     * 处理延迟消息
     */
    private void processDelayedMessages(long currentTime) {
        try {
            // 扫描所有延迟队列
            Set<String> keys = redisTemplate.keys(properties.getMessage().getKeyPrefix() + "*:delay");
            if (keys == null) return;
            
            for (String delayKey : keys) {
                // 跳过重试延迟队列
                if (delayKey.endsWith(":retry:delay")) {
                    continue;
                }
                
                // 获取到期的延迟消息
                Set<Object> expiredMessages = redisTemplate.opsForZSet()
                        .rangeByScore(delayKey, 0, currentTime);
                
                for (Object messageObj : expiredMessages) {
                    if (messageObj instanceof RedisDelayMessage) {
                        RedisDelayMessage delayMessage = (RedisDelayMessage) messageObj;
                        
                        try {
                            // 移动到正常队列
                            String normalKey = buildTopicKey(delayMessage.getTopic());
                            RedisMessage redisMessage = new RedisMessage(
                                    delayMessage.getMessageId(),
                                    delayMessage.getPayload(),
                                    System.currentTimeMillis()
                            );
                            
                            redisTemplate.opsForList().rightPush(normalKey, redisMessage);
                            
                            // 从延迟队列中移除
                            redisTemplate.opsForZSet().remove(delayKey, messageObj);
                            
                            log.debug("延迟消息已转移到正常队列 - Topic: {}, MessageId: {}", 
                                    delayMessage.getTopic(), delayMessage.getMessageId());
                            
                        } catch (Exception e) {
                            log.error("处理延迟消息失败 - MessageId: {}", delayMessage.getMessageId(), e);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("处理延迟消息失败", e);
        }
    }
    
    /**
     * 处理重试消息
     */
    private void processRetryMessages(long currentTime) {
        try {
            // 扫描所有重试延迟队列
            Set<String> retryKeys = redisTemplate.keys(properties.getMessage().getKeyPrefix() + "*:retry:delay");
            if (retryKeys == null) return;
            
            for (String retryDelayKey : retryKeys) {
                // 获取到期的重试消息
                Set<Object> expiredRetryMessages = redisTemplate.opsForZSet()
                        .rangeByScore(retryDelayKey, 0, currentTime);
                
                for (Object messageObj : expiredRetryMessages) {
                    if (messageObj instanceof RetryMessage) {
                        RetryMessage retryMessage = (RetryMessage) messageObj;
                        
                        try {
                            // 提取topic名称
                            String topic = retryDelayKey.replace(properties.getMessage().getKeyPrefix(), "")
                                    .replace(":retry:delay", "");
                            
                            // 移动到正常队列进行重试
                            String normalKey = buildTopicKey(topic);
                            redisTemplate.opsForList().rightPush(normalKey, retryMessage.getOriginalMessage());
                            
                            // 从重试延迟队列中移除
                            redisTemplate.opsForZSet().remove(retryDelayKey, messageObj);
                            
                            log.debug("重试消息已转移到正常队列 - Topic: {}, RetryCount: {}", 
                                    topic, retryMessage.getRetryCount());
                            
                        } catch (Exception e) {
                            log.error("处理重试消息失败 - RetryCount: {}", retryMessage.getRetryCount(), e);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("处理重试消息失败", e);
        }
    }
    
    // ==================== 内部类 ====================
    
    /**
     * Redis消息包装类
     */
    public static class RedisMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String messageId;
        private Object payload;
        private long timestamp;
        
        public RedisMessage() {}
        
        public RedisMessage(String messageId, Object payload, long timestamp) {
            this.messageId = messageId;
            this.payload = payload;
            this.timestamp = timestamp;
        }
        
        // Getters and Setters
        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        
        public Object getPayload() { return payload; }
        public void setPayload(Object payload) { this.payload = payload; }
        
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }
    
    /**
     * Redis延迟消息类
     */
    public static class RedisDelayMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String messageId;
        private Object payload;
        private String topic;
        private long executeTime;
        private long createTime;
        
        public RedisDelayMessage() {}
        
        public RedisDelayMessage(String messageId, Object payload, String topic, long executeTime, long createTime) {
            this.messageId = messageId;
            this.payload = payload;
            this.topic = topic;
            this.executeTime = executeTime;
            this.createTime = createTime;
        }
        
        // Getters and Setters
        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        
        public Object getPayload() { return payload; }
        public void setPayload(Object payload) { this.payload = payload; }
        
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        
        public long getExecuteTime() { return executeTime; }
        public void setExecuteTime(long executeTime) { this.executeTime = executeTime; }
        
        public long getCreateTime() { return createTime; }
        public void setCreateTime(long createTime) { this.createTime = createTime; }
    }
    
    /**
     * 重试消息类
     */
    public static class RetryMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private Object originalMessage;
        private String errorMessage;
        private int retryCount;
        private long lastRetryTime;
        
        public RetryMessage() {}
        
        public RetryMessage(Object originalMessage, String errorMessage, int retryCount, long lastRetryTime) {
            this.originalMessage = originalMessage;
            this.errorMessage = errorMessage;
            this.retryCount = retryCount;
            this.lastRetryTime = lastRetryTime;
        }
        
        // Getters and Setters
        public Object getOriginalMessage() { return originalMessage; }
        public void setOriginalMessage(Object originalMessage) { this.originalMessage = originalMessage; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
        
        public long getLastRetryTime() { return lastRetryTime; }
        public void setLastRetryTime(long lastRetryTime) { this.lastRetryTime = lastRetryTime; }
    }
}

