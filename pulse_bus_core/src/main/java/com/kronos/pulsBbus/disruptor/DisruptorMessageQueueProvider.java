package com.kronos.pulsBbus.disruptor;

import com.kronos.pulsBbus.core.MessageQueueProvider;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:51
 * @desc
 */
@org.springframework.stereotype.Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "message.queue.provider",
        havingValue = "disruptor"
)
public class DisruptorMessageQueueProvider implements MessageQueueProvider {

    private static final Logger log = LoggerFactory.getLogger(DisruptorMessageQueueProvider.class);

    @Resource
    private MessageQueueMetrics metrics;

    @Resource
    private DisruptorRetryHandler retryHandler;

    @Resource
    private          DisruptorMessageQueueTemplate template;
    private volatile boolean                       initialized = false;


    public DisruptorMessageQueueProvider() {

    }

    @Override
    public String getProviderName() {
        return "disruptor";
    }

    @Override
    public MessageQueueTemplate getTemplate() {
        if (!initialized) {
            throw new IllegalStateException("Disruptor提供者未初始化");
        }
        return template;
    }

    @Override
    public <T extends BaseProviderConfig> void initialize(T config) {
        if (initialized) {
            return;
        }
        DisruptorProperties properties = (DisruptorProperties) config;

        try {
            log.info(">>>>>> 初始化Disruptor消息队列提供者...");

            // 验证配置
            validateConfiguration(properties);

            // 创建Disruptor模板
            this.template = new DisruptorMessageQueueTemplate(properties, metrics, retryHandler);

            this.initialized = true;
            log.info("<<<<<< Disruptor消息队列提供者初始化成功");
        } catch (Exception e) {
            log.error("Disruptor消息队列提供者初始化失败", e);
            throw new RuntimeException("Disruptor提供者初始化失败", e);
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

        if (retryHandler != null) {
            retryHandler.shutdown();
        }

        initialized = false;
        log.info("Disruptor消息队列提供者已关闭");
    }

    /**
     * 验证配置
     */
    private void validateConfiguration(DisruptorProperties disruptorProperties) {
        if (!DisruptorUtils.isPowerOfTwo(disruptorProperties.getRingBufferSize())) {
            throw new IllegalArgumentException("RingBuffer大小必须是2的幂: " + disruptorProperties.getRingBufferSize());
        }

        if (disruptorProperties.getConsumerThreadPoolSize() <= 0) {
            throw new IllegalArgumentException("消费者线程池大小必须大于0: " + disruptorProperties.getConsumerThreadPoolSize());
        }

        if (disruptorProperties.getBatch().getBatchSize() <= 0) {
            throw new IllegalArgumentException("批次大小必须大于0: " + disruptorProperties.getBatch().getBatchSize());
        }
    }

    /**
     * 获取Disruptor特定的统计信息
     */
    public java.util.Map<String, Object> getDisruptorStats() {
        if (template != null) {
            return template.getStats();
        }
        return new java.util.HashMap<>();
    }

    /**
     * 获取Disruptor模板
     */
    public DisruptorMessageQueueTemplate getDisruptorTemplate() {
        return template;
    }
}

