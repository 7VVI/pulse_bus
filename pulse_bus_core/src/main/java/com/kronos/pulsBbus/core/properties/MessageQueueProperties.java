package com.kronos.pulsBbus.core.properties;

import lombok.Data;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:28
 * @desc
 */
@org.springframework.boot.context.properties.ConfigurationProperties(prefix = "message.queue")
@Data
public class MessageQueueProperties {

    private String        defaultProvider = "disruptor";
    private Providers     providers;
    private RetryPolicy   retryPolicy     = new RetryPolicy();
    private BatchConfig   batchConfig     = new BatchConfig();
    private MonitorConfig monitorConfig   = new MonitorConfig();

    @Data
    public static class RetryPolicy {
        private int    maxRetryCount     = 3;
        private long   retryDelayMs      = 1000;
        private long   maxRetryDelayMs   = 30000;
        private double backoffMultiplier = 2.0;
    }

    @Data
    public static class BatchConfig {
        private int     batchSize      = 100;
        private long    batchTimeoutMs = 1000;
        private boolean enableBatch    = true;
    }

    @Data
    public static class MonitorConfig {
        private boolean enableMetrics            = true;
        private boolean enableHealthCheck        = true;
        private long    metricsCollectIntervalMs = 60000;
    }
}
