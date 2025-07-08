package com.kronos.pulsBbus.core.properties;

import com.kronos.pulsBbus.disruptor.DisruptorProperties;
import com.kronos.pulsBbus.kafka.KafkaProperties;
import com.kronos.pulsBbus.redis.RedisProperties;
import lombok.Data;

/**
 * @author zhangyh
 * @Date 2025/6/27 17:00
 * @desc
 */
@Data
public class Providers {

    private DisruptorProperties disruptor = new DisruptorProperties();
    private KafkaProperties kafka = new KafkaProperties();
    private RedisProperties redis = new RedisProperties();
}
