package com.kronos.pulsBbus.core.batch;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:56
 * @desc
 */
public class BatchConsumeConfig {
    private int     batchSize            = 10;                    // 批次大小
    private long    batchTimeoutMs       = 1000;            // 批次超时时间
    private long    maxWaitTimeMs        = 5000;             // 最大等待时间
    private boolean enablePartialConsume = true;    // 是否允许部分消费
    private int     maxRetryCount        = 3;                 // 最大重试次数

    // Getters and Setters
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

    public long getMaxWaitTimeMs() {
        return maxWaitTimeMs;
    }

    public void setMaxWaitTimeMs(long maxWaitTimeMs) {
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    public boolean isEnablePartialConsume() {
        return enablePartialConsume;
    }

    public void setEnablePartialConsume(boolean enablePartialConsume) {
        this.enablePartialConsume = enablePartialConsume;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }
}

