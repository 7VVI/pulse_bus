package com.kronos.pulsBbus.disruptor;

import com.kronos.pulsBbus.core.ProviderEnum;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import lombok.Data;

/**
 * @author zhangyh
 * @Date 2025/6/27 16:58
 * @desc
 */

@Data
public class DisruptorProperties implements BaseProviderConfig {

    private Boolean isEnable = true;
    /**
     * RingBuffer大小，必须是2的幂
     */
    private int ringBufferSize = 1024;

    /**
     * 等待策略
     */
    private String waitStrategy ="BLOCKING";

    /**
     * 生产者类型
     */
    private String producerType = "single";

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

    @Override
    public String getName() {
        return String.valueOf(ProviderEnum.DISRUPTOR);
    }

    @Override
    public Boolean isEnable() {
        return isEnable;
    }


    /**
     * 批量配置
     */
    @Data
    public static class BatchConfig {
        private boolean enabled        = true;
        private int     batchSize      = 100;
        private long    batchTimeoutMs = 1000;
        private int     maxRetryCount  = 3;
    }

    /**
     * 重试配置
     */
    @Data
    public static class RetryConfig {
        private int    maxRetryCount   = 3;
        private long   retryDelayMs    = 1000;
        private double retryMultiplier = 2.0;
        private long   maxRetryDelayMs = 30000;
    }
}
