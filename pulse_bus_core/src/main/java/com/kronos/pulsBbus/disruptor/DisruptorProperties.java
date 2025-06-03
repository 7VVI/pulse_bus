package com.kronos.pulsBbus.disruptor;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:43
 * @desc
 */
@Component
@ConfigurationProperties(prefix = "message.queue.disruptor")
public class DisruptorProperties {

    /**
     * RingBuffer大小，必须是2的幂
     */
    private int ringBufferSize = 1024;

    /**
     * 等待策略
     */
    private WaitStrategyType waitStrategy = WaitStrategyType.BLOCKING;

    /**
     * 生产者类型
     */
    private ProducerType producerType = ProducerType.MULTI;

    /**
     * 消费者线程池大小
     */
    private int consumerThreadPoolSize = Runtime.getRuntime().availableProcessors();

    /**
     * 批量消费配置
     */
    private BatchConfig batch = new BatchConfig();

    /**
     * 重试配置
     */
    private RetryConfig retry = new RetryConfig();

    // Getters and Setters
    public int getRingBufferSize() { return ringBufferSize; }
    public void setRingBufferSize(int ringBufferSize) { this.ringBufferSize = ringBufferSize; }

    public WaitStrategyType getWaitStrategy() { return waitStrategy; }
    public void setWaitStrategy(WaitStrategyType waitStrategy) { this.waitStrategy = waitStrategy; }

    public ProducerType getProducerType() { return producerType; }
    public void setProducerType(ProducerType producerType) { this.producerType = producerType; }

    public int getConsumerThreadPoolSize() { return consumerThreadPoolSize; }
    public void setConsumerThreadPoolSize(int consumerThreadPoolSize) { this.consumerThreadPoolSize = consumerThreadPoolSize; }

    public BatchConfig getBatch() { return batch; }
    public void setBatch(BatchConfig batch) { this.batch = batch; }

    public RetryConfig getRetry() { return retry; }
    public void setRetry(RetryConfig retry) { this.retry = retry; }

    /**
     * 等待策略枚举
     */
    public enum WaitStrategyType {
        BLOCKING,           // 阻塞等待策略
        BUSY_SPIN,         // 忙等待策略
        YIELDING,          // 让步等待策略
        SLEEPING,          // 睡眠等待策略
        TIMEOUT_BLOCKING   // 超时阻塞等待策略
    }

    /**
     * 生产者类型枚举
     */
    public enum ProducerType {
        SINGLE,  // 单生产者
        MULTI    // 多生产者
    }

    /**
     * 批量配置
     */
    public static class BatchConfig {
        private boolean enabled = true;
        private int batchSize = 100;
        private long batchTimeoutMs = 1000;
        private int maxRetryCount = 3;

        // Getters and Setters
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public long getBatchTimeoutMs() { return batchTimeoutMs; }
        public void setBatchTimeoutMs(long batchTimeoutMs) { this.batchTimeoutMs = batchTimeoutMs; }
        public int getMaxRetryCount() { return maxRetryCount; }
        public void setMaxRetryCount(int maxRetryCount) { this.maxRetryCount = maxRetryCount; }
    }

    /**
     * 重试配置
     */
    public static class RetryConfig {
        private int maxRetryCount = 3;
        private long retryDelayMs = 1000;
        private double retryMultiplier = 2.0;
        private long maxRetryDelayMs = 30000;

        // Getters and Setters
        public int getMaxRetryCount() { return maxRetryCount; }
        public void setMaxRetryCount(int maxRetryCount) { this.maxRetryCount = maxRetryCount; }
        public long getRetryDelayMs() { return retryDelayMs; }
        public void setRetryDelayMs(long retryDelayMs) { this.retryDelayMs = retryDelayMs; }
        public double getRetryMultiplier() { return retryMultiplier; }
        public void setRetryMultiplier(double retryMultiplier) { this.retryMultiplier = retryMultiplier; }
        public long getMaxRetryDelayMs() { return maxRetryDelayMs; }
        public void setMaxRetryDelayMs(long maxRetryDelayMs) { this.maxRetryDelayMs = maxRetryDelayMs; }
    }
}
