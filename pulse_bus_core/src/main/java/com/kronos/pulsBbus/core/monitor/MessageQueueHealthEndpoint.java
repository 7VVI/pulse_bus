package com.kronos.pulsBbus.core.monitor;

import com.kronos.pulsBbus.core.MessageQueueManager;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:29
 * @desc
 */
@org.springframework.web.bind.annotation.RestController
@org.springframework.web.bind.annotation.RequestMapping("/actuator/mq")
public class MessageQueueHealthEndpoint {

    private final MessageQueueManager mqManager;
    private final MessageQueueMetrics metrics;

    public MessageQueueHealthEndpoint(MessageQueueManager mqManager, MessageQueueMetrics metrics) {
        this.mqManager = mqManager;
        this.metrics = metrics;
    }

    /**
     * 健康检查
     */
    @org.springframework.web.bind.annotation.GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> health = new java.util.HashMap<>();
        Map<String, Boolean> providerHealth = mqManager.healthCheck();

        boolean overallHealth = providerHealth.values().stream().allMatch(Boolean::booleanValue);
        health.put("status", overallHealth ? "UP" : "DOWN");
        health.put("providers", providerHealth);
        health.put("timestamp", System.currentTimeMillis());

        return health;
    }

    /**
     * 度量数据
     */
    @org.springframework.web.bind.annotation.GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return metrics.getMetrics();
    }
}
