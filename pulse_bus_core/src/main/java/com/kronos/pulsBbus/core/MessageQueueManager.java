package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.MessageQueueProperties;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.redis.RedisProvider;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:28
 * @desc
 */
@Component
public class MessageQueueManager {

    private final Map<String, MessageQueueProvider> providers = new java.util.concurrent.ConcurrentHashMap<>();
    private final MessageQueueProperties            properties;
    private final MessageQueueMetrics  metrics;
    private       MessageQueueTemplate defaultTemplate;

    public MessageQueueManager(MessageQueueProperties properties, MessageQueueMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
        this.initializeProviders();
    }

    /**
     * 初始化所有提供者
     */
    private void initializeProviders() {
        // 根据配置初始化提供者
        String defaultProvider = properties.getDefaultProvider();
        Map<String, Map<String, Object>> providerConfigs = properties.getProviders();

        for (Map.Entry<String, Map<String, Object>> entry : providerConfigs.entrySet()) {
            String providerName = entry.getKey();
            Map<String, Object> config = entry.getValue();

            MessageQueueProvider provider = createProvider(providerName);
            if (provider != null) {
                provider.initialize(config);
                providers.put(providerName, provider);

                if (providerName.equals(defaultProvider)) {
                    this.defaultTemplate = provider.getTemplate();
                }
            }
        }
    }

    /**
     * 创建提供者实例
     */
    private MessageQueueProvider createProvider(String providerName) {
        switch (providerName.toLowerCase()) {
            case "rabbitmq":
//                return new RabbitMQProvider();
            case "redis":
                return new RedisProvider();
            case "rocketmq":
                // return new RocketMQProvider();
            case "kafka":
                // return new KafkaProvider();
            default:
                return null;
        }
    }

    /**
     * 获取默认模板
     */
    public MessageQueueTemplate getTemplate() {
        return defaultTemplate;
    }

    /**
     * 获取指定提供者的模板
     */
    public MessageQueueTemplate getTemplate(String providerName) {
        MessageQueueProvider provider = providers.get(providerName);
        return provider != null ? provider.getTemplate() : null;
    }

    /**
     * 健康检查
     */
    public Map<String, Boolean> healthCheck() {
        Map<String, Boolean> healthStatus = new java.util.HashMap<>();
        for (Map.Entry<String, MessageQueueProvider> entry : providers.entrySet()) {
            healthStatus.put(entry.getKey(), entry.getValue().isHealthy());
        }
        return healthStatus;
    }
}