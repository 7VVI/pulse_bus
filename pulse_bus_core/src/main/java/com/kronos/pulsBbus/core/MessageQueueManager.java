package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.MessageQueueProperties;
import com.kronos.pulsBbus.core.properties.Providers;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.disruptor.DisruptorMessageQueueProvider;
import com.kronos.pulsBbus.disruptor.DisruptorProperties;
import com.kronos.pulsBbus.kafka.KafkaMessageQueueProvider;
import com.kronos.pulsBbus.kafka.KafkaProperties;
import com.kronos.pulsBbus.redis.RedisProvider;
import com.kronos.pulsBbus.redis.RedisProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:28
 * @desc
 */
public class MessageQueueManager {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueManager.class);
    
    private final Map<String, MessageQueueProvider> providers = new java.util.concurrent.ConcurrentHashMap<>();
    private final MessageQueueProperties            properties;
    private final MessageQueueMetrics               metrics;
    private       MessageQueueTemplate              defaultTemplate;

    public MessageQueueManager(MessageQueueProperties properties) {
        this.properties = properties;
        this.metrics = new MessageQueueMetrics();
        this.initializeProviders();
    }
    
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
        Providers propertiesProviders = properties.getProviders();
        
        // 初始化Disruptor提供者
        DisruptorProperties disruptor = propertiesProviders.getDisruptor();
        Boolean isDisruptorEnabled = disruptor.isEnable();
        if (isDisruptorEnabled) {
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
        
        // 初始化Kafka提供者
        KafkaProperties kafka = propertiesProviders.getKafka();
        Boolean isKafkaEnabled = kafka.isEnable();
        if (isKafkaEnabled) {
            String providerName = kafka.getName();
            MessageQueueProvider provider = createProvider(providerName);
            if (provider != null) {
                provider.initialize(kafka);
                providers.put(providerName, provider);

                if (providerName.equals(defaultProvider)) {
                    this.defaultTemplate = provider.getTemplate();
                }
            }
        }
        
        // 初始化Redis提供者
        RedisProperties redis = propertiesProviders.getRedis();
        Boolean isRedisEnabled = redis.isEnable();
        if (isRedisEnabled) {
            try {
                String providerName = redis.getName();
                RedisProvider redisProvider = new RedisProvider();
                redisProvider.initialize(redis);
                providers.put(providerName, redisProvider);
                
                if (providerName.equals(defaultProvider)) {
                    this.defaultTemplate = redisProvider.getTemplate();
                }
                
                log.info("Redis消息队列提供者初始化成功");
            } catch (Exception e) {
                log.error("Redis消息队列提供者初始化失败", e);
                // Redis提供者初始化失败时的处理
                throw new RuntimeException("Redis消息队列提供者初始化失败", e);
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
                return new KafkaMessageQueueProvider();
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