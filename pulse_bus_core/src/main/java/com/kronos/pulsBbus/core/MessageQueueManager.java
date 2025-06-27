package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.properties.MessageQueueProperties;
import com.kronos.pulsBbus.core.properties.Providers;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.disruptor.DisruptorMessageQueueProvider;
import com.kronos.pulsBbus.disruptor.DisruptorProperties;
import com.kronos.pulsBbus.redis.RedisProvider;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:28
 * @desc
 */
public class MessageQueueManager {

    private final Map<String, MessageQueueProvider> providers = new java.util.concurrent.ConcurrentHashMap<>();
    private final MessageQueueProperties            properties;
    private       MessageQueueTemplate              defaultTemplate;

    public MessageQueueManager(MessageQueueProperties properties) {
        this.properties = properties;
        this.initializeProviders();
    }

    /**
     * 初始化所有提供者
     */
    private void initializeProviders() {
        // 根据配置初始化提供者
        String defaultProvider = properties.getDefaultProvider();
        Providers propertiesProviders = properties.getProviders();
        DisruptorProperties disruptor = propertiesProviders.getDisruptor();
        Boolean isEnabled = disruptor.getIsEnable();
        if (isEnabled) {
            String providerName = disruptor.getName();
            MessageQueueProvider provider = createProvider(providerName);
            if (provider != null) {
                provider.initialize(disruptor);
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
            case "disruptor":
                return new DisruptorMessageQueueProvider();
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