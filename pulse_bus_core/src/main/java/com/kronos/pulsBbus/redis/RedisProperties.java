package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.ProviderEnum;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import lombok.Data;

/**
 * @author zhangyh
 * @Date 2025/6/3 10:00
 * @desc Redis消息队列配置属性
 */
@Data
public class RedisProperties implements BaseProviderConfig {

    private String       name     = "redis";
    private Boolean      isEnable = false;
    private ProviderEnum provider = ProviderEnum.REDIS;

    // Redis连接配置
    private String host     = "localhost";
    private int    port     = 6379;
    private String password;
    private int    database = 0;
    private int    timeout  = 2000;

    // 连接池配置
    private PoolConfig pool = new PoolConfig();

    // 消息配置
    private MessageConfig message = new MessageConfig();

    // 延迟消息配置
    private DelayConfig delay = new DelayConfig();

    // 批处理配置
    private BatchConfig batch = new BatchConfig();

    // 重试配置
    private RetryConfig retry = new RetryConfig();

    // 监控配置
    private MonitorConfig monitor = new MonitorConfig();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Boolean isEnable() {
        return isEnable;
    }

    @Data
    public static class PoolConfig {
        private int maxTotal     = 8;
        private int maxIdle      = 8;
        private int minIdle      = 0;
        private int maxWaitMs    = -1;
        private boolean testOnBorrow = false;
        private boolean testOnReturn = false;
        private boolean testWhileIdle = true;
    }

    @Data
    public static class MessageConfig {
        private String keyPrefix        = "pulse:bus:";
        private String delayKeyPrefix   = "pulse:bus:delay:";
        private long   defaultTtl       = 86400000; // 24小时
        private String serializer       = "json";
        private boolean enableCompression = false;
    }

    @Data
    public static class DelayConfig {
        private boolean enabled           = true;
        private long    scanIntervalMs    = 1000;
        private int     batchSize         = 100;
        private String  delayQueueKey     = "pulse:bus:delay:queue";
        private int     workerThreads     = 2;
    }

    @Data
    public static class BatchConfig {
        private boolean enabled        = true;
        private int     batchSize      = 100;
        private long    batchTimeoutMs = 1000;
        private int     maxRetryCount  = 3;
    }

    @Data
    public static class RetryConfig {
        private int    maxRetryCount   = 3;
        private long   retryDelayMs    = 1000;
        private double retryMultiplier = 2.0;
        private long   maxRetryDelayMs = 30000;
        private String retryQueueKey   = "pulse:bus:retry:queue";
    }

    @Data
    public static class MonitorConfig {
        private boolean enabled           = true;
        private long    metricsIntervalMs = 30000;
        private String  metricsKey        = "pulse:bus:metrics";
    }
}