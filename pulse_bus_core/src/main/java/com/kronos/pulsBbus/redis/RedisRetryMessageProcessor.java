package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.retry.RetryMessage;
import com.kronos.pulsBbus.core.retry.RetryMessageProcessor;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis重试消息处理器实现
 * 
 * @author system
 * @since 1.0.0
 */
public class RedisRetryMessageProcessor implements RetryMessageProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(RedisRetryMessageProcessor.class);
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisProperties properties;
    private final MessageQueueMetrics metrics;
    private final ScheduledExecutorService retryExecutor;
    
    private volatile boolean running = false;
    
    public RedisRetryMessageProcessor(RedisTemplate<String, Object> redisTemplate,
                                    RedisProperties properties,
                                    MessageQueueMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metrics = metrics;
        this.retryExecutor = Executors.newScheduledThreadPool(1,
                r -> new Thread(r, "redis-retry-processor-"));
    }
    
    @Override
    public void handleRetry(String topic, Object message, MessageConsumer consumer, Exception error) {
        if (properties.getRetry().getMaxRetryCount() <= 0) {
            return;
        }
        
        try {
            String messageId = UUID.randomUUID().toString();
            
            // 检查是否已经是重试消息
            int currentRetryCount = 1;
            Object originalMessage = message;
            
            if (message instanceof RetryMessage) {
                RetryMessage existingRetry = (RetryMessage) message;
                currentRetryCount = existingRetry.getRetryCount() + 1;
                originalMessage = existingRetry.getOriginalMessage();
                messageId = existingRetry.getMessageId();
                
                // 检查是否超过最大重试次数
                if (currentRetryCount > properties.getRetry().getMaxRetryCount()) {
                    log.warn("消息重试次数已达上限，丢弃消息 - Topic: {}, RetryCount: {}", 
                            topic, currentRetryCount - 1);
                    metrics.recordConsumeFailed(topic);
                    return;
                }
            }
            
            RetryMessage retryMessage = new RetryMessage(
                    messageId, originalMessage, topic, error.getMessage(), currentRetryCount);
            retryMessage.setMaxRetryCount(properties.getRetry().getMaxRetryCount());
            
            // 计算下次重试时间
            long retryDelay = calculateRetryDelay(currentRetryCount);
            long nextRetryTime = System.currentTimeMillis() + retryDelay;
            retryMessage.setNextRetryTime(nextRetryTime);
            
            // 添加到延迟重试队列
            String retryDelayKey = buildRetryDelayKey(topic);
            redisTemplate.opsForZSet().add(retryDelayKey, retryMessage, nextRetryTime);
            
            log.debug("消息已添加到重试队列 - Topic: {}, MessageId: {}, RetryCount: {}, NextRetryTime: {}",
                    topic, messageId, currentRetryCount, nextRetryTime);
            
            metrics.recordConsumeRetry(topic);
            
        } catch (Exception e) {
            log.error("添加重试消息失败 - Topic: {}", topic, e);
            metrics.recordConsumeFailed(topic);
        }
    }
    
    @Override
    public void handleRetry(Message message, MessageConsumer consumer, ConsumeResult result) {
        if (result != ConsumeResult.RETRY) {
            return;
        }
        
        int currentRetryCount = message.getRetryCount();
        
        if (currentRetryCount >= properties.getRetry().getMaxRetryCount()) {
            // 超过最大重试次数，记录失败
            metrics.recordConsumeFailed(message.getTopic());
            log.warn("消息重试次数已达上限，丢弃消息 - Topic: {}, MessageId: {}, RetryCount: {}",
                    message.getTopic(), message.getId(), currentRetryCount);
            return;
        }
        
        try {
            RetryMessage retryMessage = new RetryMessage(
                    message.getId(), message.getPayload(), message.getTopic(), 
                    "消费失败需要重试", currentRetryCount + 1);
            retryMessage.setMaxRetryCount(properties.getRetry().getMaxRetryCount());
            
            // 计算下次重试时间
            long retryDelay = calculateRetryDelay(currentRetryCount + 1);
            long nextRetryTime = System.currentTimeMillis() + retryDelay;
            retryMessage.setNextRetryTime(nextRetryTime);
            
            // 添加到延迟重试队列
            String retryDelayKey = buildRetryDelayKey(message.getTopic());
            redisTemplate.opsForZSet().add(retryDelayKey, retryMessage, nextRetryTime);
            
            log.debug("消息已添加到重试队列 - Topic: {}, MessageId: {}, RetryCount: {}",
                    message.getTopic(), message.getId(), currentRetryCount + 1);
            
            metrics.recordConsumeRetry(message.getTopic());
            
        } catch (Exception e) {
            log.error("添加重试消息失败 - Topic: {}, MessageId: {}", 
                    message.getTopic(), message.getId(), e);
            metrics.recordConsumeFailed(message.getTopic());
        }
    }
    
    @Override
    public void start() {
        if (running) {
            return;
        }
        
        if (properties.getRetry().getMaxRetryCount() > 0) {
            retryExecutor.scheduleWithFixedDelay(
                () -> processExpiredRetryMessages(System.currentTimeMillis()),
                0,
                properties.getRetry().getScanIntervalMs(),
                TimeUnit.MILLISECONDS
            );
            
            running = true;
            log.info("Redis重试消息处理器已启动，扫描间隔: {}ms", properties.getRetry().getScanIntervalMs());
        }
    }
    
    @Override
    public void stop() {
        running = false;
        if (retryExecutor != null && !retryExecutor.isShutdown()) {
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
        log.info("Redis重试消息处理器已停止");
    }
    
    @Override
    public void processExpiredRetryMessages(long currentTime) {
        if (!running) {
            return;
        }
        
        try {
            // 扫描所有重试延迟队列
            Set<String> retryKeys = redisTemplate.keys(
                    properties.getMessage().getKeyPrefix() + "*:retry:delay");
            
            if (retryKeys == null || retryKeys.isEmpty()) {
                return;
            }
            
            for (String retryDelayKey : retryKeys) {
                processRetryQueue(retryDelayKey, currentTime);
            }
            
        } catch (Exception e) {
            log.error("处理重试消息失败", e);
        }
    }
    
    @Override
    public long getRetryMessageCount(String topic) {
        try {
            String retryDelayKey = buildRetryDelayKey(topic);
            Long count = redisTemplate.opsForZSet().count(retryDelayKey, 
                    Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            return count != null ? count : 0;
        } catch (Exception e) {
            log.error("获取重试消息数量失败 - Topic: {}", topic, e);
            return 0;
        }
    }
    
    @Override
    public boolean cancelRetryMessage(String topic, String messageId) {
        try {
            String retryDelayKey = buildRetryDelayKey(topic);
            
            // 获取所有重试消息，查找匹配的消息ID
            Set<Object> messages = redisTemplate.opsForZSet().range(retryDelayKey, 0, -1);
            if (messages == null) {
                return false;
            }
            
            for (Object messageObj : messages) {
                if (messageObj instanceof RetryMessage) {
                    RetryMessage retryMessage = (RetryMessage) messageObj;
                    if (messageId.equals(retryMessage.getMessageId())) {
                        Long removed = redisTemplate.opsForZSet().remove(retryDelayKey, messageObj);
                        boolean success = removed != null && removed > 0;
                        
                        if (success) {
                            log.debug("重试消息已取消 - Topic: {}, MessageId: {}", topic, messageId);
                        }
                        
                        return success;
                    }
                }
            }
            
            return false;
            
        } catch (Exception e) {
            log.error("取消重试消息失败 - Topic: {}, MessageId: {}", topic, messageId, e);
            return false;
        }
    }
    
    @Override
    public long calculateRetryDelay(int retryCount) {
        // 指数退避算法
        long baseDelay = properties.getRetry().getRetryDelayMs();
        double multiplier = properties.getRetry().getRetryMultiplier();
        long maxDelay = properties.getRetry().getMaxRetryDelayMs();
        
        long delay = (long) (baseDelay * Math.pow(multiplier, retryCount - 1));
        return Math.min(delay, maxDelay);
    }
    
    /**
     * 处理单个重试队列
     */
    private void processRetryQueue(String retryDelayKey, long currentTime) {
        try {
            // 获取到期的重试消息
            Set<Object> expiredRetryMessages = redisTemplate.opsForZSet()
                    .rangeByScore(retryDelayKey, 0, currentTime);
            
            if (expiredRetryMessages == null || expiredRetryMessages.isEmpty()) {
                return;
            }
            
            for (Object messageObj : expiredRetryMessages) {
                if (messageObj instanceof RetryMessage) {
                    RetryMessage retryMessage = (RetryMessage) messageObj;
                    
                    try {
                        // 提取topic名称
                        String topic = extractTopicFromRetryKey(retryDelayKey);
                        
                        // 移动到正常队列进行重试
                        moveToNormalQueue(topic, retryMessage);
                        
                        // 从重试延迟队列中移除
                        redisTemplate.opsForZSet().remove(retryDelayKey, messageObj);
                        
                        log.debug("重试消息已转移到正常队列 - Topic: {}, MessageId: {}, RetryCount: {}",
                                topic, retryMessage.getMessageId(), retryMessage.getRetryCount());
                        
                        metrics.recordRetryMessageProcessed(topic);
                        
                    } catch (Exception e) {
                        log.error("处理重试消息失败 - MessageId: {}, RetryCount: {}",
                                retryMessage.getMessageId(), retryMessage.getRetryCount(), e);
                        metrics.recordRetryMessageFailed(retryMessage.getTopic());
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("处理重试队列失败 - RetryDelayKey: {}", retryDelayKey, e);
        }
    }
    
    /**
     * 将重试消息移动到正常队列
     */
    private void moveToNormalQueue(String topic, RetryMessage retryMessage) {
        String normalKey = buildTopicKey(topic);
        
        // 创建Redis消息对象
        RedisMessage redisMessage = new RedisMessage(
                retryMessage.getMessageId(),
                retryMessage.getOriginalMessage(),
                System.currentTimeMillis(),
                retryMessage.getRetryCount()
        );
        
        redisTemplate.opsForList().rightPush(normalKey, redisMessage);
    }
    
    /**
     * 从重试键名中提取topic名称
     */
    private String extractTopicFromRetryKey(String retryDelayKey) {
        return retryDelayKey.replace(properties.getMessage().getKeyPrefix(), "")
                .replace(":retry:delay", "");
    }
    
    /**
     * 构建Topic键名
     */
    private String buildTopicKey(String topic) {
        return properties.getMessage().getKeyPrefix() + topic;
    }
    
    /**
     * 构建重试延迟键名
     */
    private String buildRetryDelayKey(String topic) {
        return buildTopicKey(topic) + ":retry:delay";
    }
    
    @PreDestroy
    public void destroy() {
        stop();
    }
}