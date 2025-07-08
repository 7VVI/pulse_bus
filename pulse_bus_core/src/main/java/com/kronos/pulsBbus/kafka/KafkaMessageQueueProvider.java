package com.kronos.pulsBbus.kafka;

import com.kronos.pulsBbus.core.MessageQueueProvider;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyh
 * @Date 2025/6/3 9:20
 * @desc Kafka消息队列提供者
 */
@org.springframework.stereotype.Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "message.queue.provider",
        havingValue = "kafka"
)
public class KafkaMessageQueueProvider implements MessageQueueProvider {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageQueueProvider.class);

    @Resource
    private MessageQueueMetrics metrics;

    private KafkaMessageQueueTemplate template;
    private volatile boolean initialized = false;

    public KafkaMessageQueueProvider() {
    }

    @Override
    public String getProviderName() {
        return "kafka";
    }

    @Override
    public MessageQueueTemplate getTemplate() {
        if (!initialized) {
            throw new IllegalStateException("Kafka提供者未初始化");
        }
        return template;
    }

    @Override
    public <T extends BaseProviderConfig> void initialize(T config) {
        if (initialized) {
            return;
        }
        
        KafkaProperties properties = (KafkaProperties) config;

        try {
            log.info(">>>>>> 初始化Kafka消息队列提供者...");

            // 验证配置
            validateConfiguration(properties);

            // 创建Kafka模板
            this.template = new KafkaMessageQueueTemplate(properties, metrics);

            this.initialized = true;
            log.info("<<<<<< Kafka消息队列提供者初始化成功");
        } catch (Exception e) {
            log.error("Kafka消息队列提供者初始化失败", e);
            throw new RuntimeException("Kafka提供者初始化失败", e);
        }
    }

    @Override
    public boolean isHealthy() {
        return initialized && template != null;
    }

    @Override
    public void destroy() {
        if (template != null) {
            template.shutdown();
        }

        initialized = false;
        log.info("Kafka消息队列提供者已关闭");
    }

    /**
     * 验证配置
     */
    private void validateConfiguration(KafkaProperties kafkaProperties) {
        if (kafkaProperties.getBootstrapServers() == null || kafkaProperties.getBootstrapServers().trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap.servers不能为空");
        }

        if (kafkaProperties.getGroupId() == null || kafkaProperties.getGroupId().trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka group.id不能为空");
        }

        if (kafkaProperties.getBatch().getBatchSize() <= 0) {
            throw new IllegalArgumentException("批次大小必须大于0: " + kafkaProperties.getBatch().getBatchSize());
        }

        if (kafkaProperties.getRetry().getMaxRetryCount() < 0) {
            throw new IllegalArgumentException("最大重试次数不能小于0: " + kafkaProperties.getRetry().getMaxRetryCount());
        }
    }

    /**
     * 获取Kafka特定的统计信息
     */
    public java.util.Map<String, Object> getKafkaStats() {
        if (template != null) {
            return template.getStats();
        }
        return new java.util.HashMap<>();
    }

    /**
     * 获取Kafka模板
     */
    public KafkaMessageQueueTemplate getKafkaTemplate() {
        return template;
    }
}