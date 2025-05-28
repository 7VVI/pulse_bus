package com.kronos.pulsBbus.core.properties;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:28
 * @desc
 */
@org.springframework.boot.context.properties.ConfigurationProperties(prefix = "message.queue")
@org.springframework.stereotype.Component
public class MessageQueueProperties {

    private String                           defaultProvider = "redis";
    private Map<String, Map<String, Object>> providers       = new java.util.HashMap<>();
    private RetryPolicy                      retryPolicy     = new RetryPolicy();
    private BatchConfig                      batchConfig     = new BatchConfig();
    private MonitorConfig                    monitorConfig   = new MonitorConfig();

    public static class RetryPolicy {
        private int    maxRetryCount     = 3;
        private long   retryDelayMs      = 1000;
        private long   maxRetryDelayMs   = 30000;
        private double backoffMultiplier = 2.0;

        // getter/setter
        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public void setMaxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }

        public long getRetryDelayMs() {
            return retryDelayMs;
        }

        public void setRetryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
        }

        public long getMaxRetryDelayMs() {
            return maxRetryDelayMs;
        }

        public void setMaxRetryDelayMs(long maxRetryDelayMs) {
            this.maxRetryDelayMs = maxRetryDelayMs;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }
    }

    public static class BatchConfig {
        private int     batchSize      = 100;
        private long    batchTimeoutMs = 1000;
        private boolean enableBatch    = true;

        // getter/setter
        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getBatchTimeoutMs() {
            return batchTimeoutMs;
        }

        public void setBatchTimeoutMs(long batchTimeoutMs) {
            this.batchTimeoutMs = batchTimeoutMs;
        }

        public boolean isEnableBatch() {
            return enableBatch;
        }

        public void setEnableBatch(boolean enableBatch) {
            this.enableBatch = enableBatch;
        }
    }

    public static class MonitorConfig {
        private boolean enableMetrics            = true;
        private boolean enableHealthCheck        = true;
        private long    metricsCollectIntervalMs = 60000;

        // getter/setter
        public boolean isEnableMetrics() {
            return enableMetrics;
        }

        public void setEnableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
        }

        public boolean isEnableHealthCheck() {
            return enableHealthCheck;
        }

        public void setEnableHealthCheck(boolean enableHealthCheck) {
            this.enableHealthCheck = enableHealthCheck;
        }

        public long getMetricsCollectIntervalMs() {
            return metricsCollectIntervalMs;
        }

        public void setMetricsCollectIntervalMs(long metricsCollectIntervalMs) {
            this.metricsCollectIntervalMs = metricsCollectIntervalMs;
        }
    }

    public static class DisruptorConfig {
        private int     bufferSize      = 1024;
        private String  waitStrategy    = "BlockingWaitStrategy";
        private int     consumerThreads = 1;
        private boolean enableMetrics   = true;

        // Getters and Setters
        public int getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        public String getWaitStrategy() {
            return waitStrategy;
        }

        public void setWaitStrategy(String waitStrategy) {
            this.waitStrategy = waitStrategy;
        }

        public int getConsumerThreads() {
            return consumerThreads;
        }

        public void setConsumerThreads(int consumerThreads) {
            this.consumerThreads = consumerThreads;
        }

        public boolean isEnableMetrics() {
            return enableMetrics;
        }

        public void setEnableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
        }
    }


    // 主要属性的getter/setter
    public String getDefaultProvider() {
        return defaultProvider;
    }

    public void setDefaultProvider(String defaultProvider) {
        this.defaultProvider = defaultProvider;
    }

    public Map<String, Map<String, Object>> getProviders() {
        return providers;
    }

    public void setProviders(Map<String, Map<String, Object>> providers) {
        this.providers = providers;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public BatchConfig getBatchConfig() {
        return batchConfig;
    }

    public void setBatchConfig(BatchConfig batchConfig) {
        this.batchConfig = batchConfig;
    }

    public MonitorConfig getMonitorConfig() {
        return monitorConfig;
    }

    public void setMonitorConfig(MonitorConfig monitorConfig) {
        this.monitorConfig = monitorConfig;
    }
}
