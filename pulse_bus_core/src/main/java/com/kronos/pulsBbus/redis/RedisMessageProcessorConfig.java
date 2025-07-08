package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.delay.DelayMessageProcessor;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.retry.RetryMessageProcessor;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Redis消息处理器配置类
 * 负责配置延迟消息和重试消息处理器
 * 
 * @author zhangyh
 * @Date 2025/5/28 16:30
 */
@Configuration
public class RedisMessageProcessorConfig {

    /**
     * 配置Redis延迟消息处理器
     */
    @Bean
    @ConditionalOnProperty(prefix = "pulse.bus.redis.delay", name = "enabled", havingValue = "true")
    public DelayMessageProcessor redisDelayMessageProcessor(
            RedisTemplate<String, Object> redisTemplate,
            RedisProperties properties,
            MessageQueueMetrics metrics) {
        return new RedisDelayMessageProcessor(redisTemplate, properties, metrics);
    }

    /**
     * 配置Redis重试消息处理器
     */
    @Bean
    @ConditionalOnProperty(prefix = "pulse.bus.redis.retry", name = "maxRetryCount", matchIfMissing = false)
    public RetryMessageProcessor redisRetryMessageProcessor(
            RedisTemplate<String, Object> redisTemplate,
            RedisProperties properties,
            MessageQueueMetrics metrics) {
        return new RedisRetryMessageProcessor(redisTemplate, properties, metrics);
    }
}