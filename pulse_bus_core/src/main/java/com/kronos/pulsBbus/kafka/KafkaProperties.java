package com.kronos.pulsBbus.kafka;

import com.kronos.pulsBbus.core.ProviderEnum;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import lombok.Data;

/**
 * @author zhangyh
 * @Date 2025/6/3 9:00
 * @desc Kafka配置属性
 */
@Data
public class KafkaProperties implements BaseProviderConfig {

    private String       name     = "kafka";
    private Boolean      isEnable = false;
    private ProviderEnum provider = ProviderEnum.KAFKA;

    // Kafka连接配置
    private String bootstrapServers = "localhost:9092";
    private String groupId          = "pulse-bus-group";
    private String clientId         = "pulse-bus-client";

    // 生产者配置
    private ProducerConfig producer = new ProducerConfig();

    // 消费者配置
    private ConsumerConfig consumer = new ConsumerConfig();

    // 批量配置
    private BatchConfig batch = new BatchConfig();

    // 重试配置
    private RetryConfig retry = new RetryConfig();

    @Data
    public static class ProducerConfig {
        private int     retries                          = 3;
        private int     batchSize                        = 16384;
        private long    lingerMs                         = 1;
        private long    bufferMemory                     = 33554432L;
        private String  keySerializer                    = "org.apache.kafka.common.serialization.StringSerializer";
        private String  valueSerializer                  = "org.apache.kafka.common.serialization.StringSerializer";
        private String  acks                             = "all";
        private boolean enableIdempotence                = true;
        private int     maxInFlightRequestsPerConnection = 5;
    }

    @Data
    public static class ConsumerConfig {
        private String  autoOffsetReset     = "earliest";
        private boolean enableAutoCommit    = false;
        private int     maxPollRecords      = 500;
        private long    maxPollIntervalMs   = 300000L;
        private long    sessionTimeoutMs    = 30000L;
        private long    heartbeatIntervalMs = 3000L;
        private String  keyDeserializer     = "org.apache.kafka.common.serialization.StringDeserializer";
        private String  valueDeserializer   = "org.apache.kafka.common.serialization.StringDeserializer";
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
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Boolean isEnable() {
        return isEnable;
    }

    public Boolean getIsEnable() {
        return isEnable;
    }
}