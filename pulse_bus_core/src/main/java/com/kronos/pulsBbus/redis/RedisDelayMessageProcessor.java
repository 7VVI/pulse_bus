package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.delay.DelayMessage;
import com.kronos.pulsBbus.core.delay.DelayMessageProcessor;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
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
 * Redis延迟消息处理器实现
 * 
 * @author system
 * @since 1.0.0
 */
public class RedisDelayMessageProcessor implements DelayMessageProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(RedisDelayMessageProcessor.class);
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisProperties properties;
    private final MessageQueueMetrics metrics;
    private final ScheduledExecutorService delayExecutor;
    
    private volatile boolean running = false;
    
    public RedisDelayMessageProcessor(RedisTemplate<String, Object> redisTemplate, 
                                    RedisProperties properties,
                                    MessageQueueMetrics metrics) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.metrics = metrics;
        this.delayExecutor = Executors.newScheduledThreadPool(1, 
                r -> new Thread(r, "redis-delay-processor-"));
    }
    
    @Override
    public String sendDelayMessage(String topic, Object message, long delayMs) {
        String messageId = UUID.randomUUID().toString();
        long executeTime = System.currentTimeMillis() + delayMs;
        
        DelayMessage delayMessage = new DelayMessage(messageId, message, topic, executeTime);
        
        try {
            String delayKey = buildDelayKey(topic);
            redisTemplate.opsForZSet().add(delayKey, delayMessage, executeTime);
            
            log.debug("延迟消息已发送 - Topic: {}, MessageId: {}, DelayMs: {}", 
                    topic, messageId, delayMs);
            
            metrics.recordMessageSent(topic);
            return messageId;
            
        } catch (Exception e) {
            log.error("发送延迟消息失败 - Topic: {}, MessageId: {}", topic, messageId, e);
            metrics.recordMessageSendFailed(topic);
            throw new RuntimeException("发送延迟消息失败", e);
        }
    }
    
    @Override
    public void start() {
        if (running) {
            return;
        }
        
        if (properties.getDelay().isEnabled()) {
            delayExecutor.scheduleWithFixedDelay(
                () -> processExpiredMessages(System.currentTimeMillis()),
                0,
                properties.getDelay().getScanIntervalMs(),
                TimeUnit.MILLISECONDS
            );
            
            running = true;
            log.info("Redis延迟消息处理器已启动，扫描间隔: {}ms", properties.getDelay().getScanIntervalMs());
        }
    }
    
    @Override
    public void stop() {
        running = false;
        if (delayExecutor != null && !delayExecutor.isShutdown()) {
            delayExecutor.shutdown();
            try {
                if (!delayExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    delayExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                delayExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("Redis延迟消息处理器已停止");
    }
    
    @Override
    public void processExpiredMessages(long currentTime) {
        if (!running) {
            return;
        }
        
        try {
            // 扫描所有延迟队列
            Set<String> keys = redisTemplate.keys(properties.getMessage().getKeyPrefix() + "*:delay");
            if (keys.isEmpty()) {
                return;
            }
            
            for (String delayKey : keys) {
                // 跳过重试延迟队列
                if (delayKey.endsWith(":retry:delay")) {
                    continue;
                }
                
                processDelayQueue(delayKey, currentTime);
            }
            
        } catch (Exception e) {
            log.error("处理延迟消息失败", e);
        }
    }
    
    @Override
    public long getDelayMessageCount(String topic) {
        try {
            String delayKey = buildDelayKey(topic);
            Long count = redisTemplate.opsForZSet().count(delayKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            return count != null ? count : 0;
        } catch (Exception e) {
            log.error("获取延迟消息数量失败 - Topic: {}", topic, e);
            return 0;
        }
    }
    
    @Override
    public boolean cancelDelayMessage(String topic, String messageId) {
        try {
            String delayKey = buildDelayKey(topic);
            
            // 获取所有延迟消息，查找匹配的消息ID
            Set<Object> messages = redisTemplate.opsForZSet().range(delayKey, 0, -1);
            if (messages == null) {
                return false;
            }
            
            for (Object messageObj : messages) {
                if (messageObj instanceof DelayMessage) {
                    DelayMessage delayMessage = (DelayMessage) messageObj;
                    if (messageId.equals(delayMessage.getMessageId())) {
                        Long removed = redisTemplate.opsForZSet().remove(delayKey, messageObj);
                        boolean success = removed != null && removed > 0;
                        
                        if (success) {
                            log.debug("延迟消息已取消 - Topic: {}, MessageId: {}", topic, messageId);
                        }
                        
                        return success;
                    }
                }
            }
            
            return false;
            
        } catch (Exception e) {
            log.error("取消延迟消息失败 - Topic: {}, MessageId: {}", topic, messageId, e);
            return false;
        }
    }
    
    /**
     * 处理单个延迟队列
     */
    private void processDelayQueue(String delayKey, long currentTime) {
        try {
            // 获取到期的延迟消息
            Set<Object> expiredMessages = redisTemplate.opsForZSet()
                    .rangeByScore(delayKey, 0, currentTime);
            
            if (expiredMessages == null || expiredMessages.isEmpty()) {
                return;
            }
            
            for (Object messageObj : expiredMessages) {
                if (messageObj instanceof DelayMessage) {
                    DelayMessage delayMessage = (DelayMessage) messageObj;
                    
                    try {
                        // 移动到正常队列
                        moveToNormalQueue(delayMessage);
                        
                        // 从延迟队列中移除
                        redisTemplate.opsForZSet().remove(delayKey, messageObj);
                        
                        log.debug("延迟消息已转移到正常队列 - Topic: {}, MessageId: {}", 
                                delayMessage.getTopic(), delayMessage.getMessageId());
                        
                        metrics.recordDelayMessageProcessed(delayMessage.getTopic());
                        
                    } catch (Exception e) {
                        log.error("处理延迟消息失败 - MessageId: {}", delayMessage.getMessageId(), e);
                        metrics.recordDelayMessageFailed(delayMessage.getTopic());
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("处理延迟队列失败 - DelayKey: {}", delayKey, e);
        }
    }
    
    /**
     * 将延迟消息移动到正常队列
     */
    private void moveToNormalQueue(DelayMessage delayMessage) {
        String normalKey = buildTopicKey(delayMessage.getTopic());
        
        // 创建Redis消息对象
        RedisMessage redisMessage = new RedisMessage(
                delayMessage.getMessageId(),
                delayMessage.getPayload(),
                System.currentTimeMillis()
        );
        
        redisTemplate.opsForList().rightPush(normalKey, redisMessage);
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
    
    @PreDestroy
    public void destroy() {
        stop();
    }
}