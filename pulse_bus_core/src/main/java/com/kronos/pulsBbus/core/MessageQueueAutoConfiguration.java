package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.monitor.MessageQueueHealthEndpoint;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.MessageQueueProperties;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:29
 * @desc
 */
@org.springframework.context.annotation.Configuration
@org.springframework.boot.context.properties.EnableConfigurationProperties(MessageQueueProperties.class)
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        prefix = "message.queue",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class MessageQueueAutoConfiguration {

    @org.springframework.context.annotation.Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public MessageQueueMetrics messageQueueMetrics() {
        return new MessageQueueMetrics();
    }

    @org.springframework.context.annotation.Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public MessageQueueManager messageQueueManager(MessageQueueProperties properties, MessageQueueMetrics metrics) {
        return new MessageQueueManager(properties, metrics);
    }

    @org.springframework.context.annotation.Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
    public MessageQueueTemplate messageQueueTemplate(MessageQueueManager manager) {
        return manager.getTemplate();
    }

    @org.springframework.context.annotation.Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            prefix = "message.queue.monitor",
            name = "enable-health-check",
            havingValue = "true"
    )
    public MessageQueueHealthEndpoint messageQueueHealthEndpoint(MessageQueueManager manager, MessageQueueMetrics metrics) {
        return new MessageQueueHealthEndpoint(manager, metrics);
    }
}
