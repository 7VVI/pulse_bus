package com.kronos.pulsBbus.core.monitor;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:29
 * @desc
 */
public class MessageQueueMetrics {

    // 原有指标
    private final java.util.concurrent.atomic.AtomicLong totalSentMessages = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalReceivedMessages = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalFailedMessages = new java.util.concurrent.atomic.AtomicLong(0);
    private final Map<String, java.util.concurrent.atomic.AtomicLong> topicMetrics = new java.util.concurrent.ConcurrentHashMap<>();

    // 延迟相关指标
    private final Map<String, LatencyMetrics> sendLatencyMetrics = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LatencyMetrics> consumeLatencyMetrics = new java.util.concurrent.ConcurrentHashMap<>();

    // 批量操作指标
    private final Map<String, java.util.concurrent.atomic.AtomicLong> batchMetrics = new java.util.concurrent.ConcurrentHashMap<>();

    // 消费结果指标
    private final Map<String, java.util.concurrent.atomic.AtomicLong> consumeResultMetrics = new java.util.concurrent.ConcurrentHashMap<>();

    // 提供者健康指标
    private final Map<String, java.util.concurrent.atomic.AtomicLong> providerHealthMetrics = new java.util.concurrent.ConcurrentHashMap<>();


    public void recordSentMessage(String topic) {
        totalSentMessages.incrementAndGet();
        topicMetrics.computeIfAbsent(topic + ":sent", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    public void recordReceivedMessage(String topic) {
        totalReceivedMessages.incrementAndGet();
        topicMetrics.computeIfAbsent(topic + ":received", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    public void recordFailedMessage(String topic) {
        totalFailedMessages.incrementAndGet();
        topicMetrics.computeIfAbsent(topic + ":failed", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new java.util.HashMap<>();
        metrics.put("totalSent", totalSentMessages.get());
        metrics.put("totalReceived", totalReceivedMessages.get());
        metrics.put("totalFailed", totalFailedMessages.get());

        Map<String, Long> topicStats = new java.util.HashMap<>();
        for (Map.Entry<String, java.util.concurrent.atomic.AtomicLong> entry : topicMetrics.entrySet()) {
            topicStats.put(entry.getKey(), entry.getValue().get());
        }
        metrics.put("topics", topicStats);

        // 新增延迟指标到返回结果
        metrics.put("sendLatency", getSendLatencyMetrics());
        metrics.put("consumeLatency", getConsumeLatencyMetrics());
        metrics.put("batchMetrics", getBatchMetrics());
        metrics.put("consumeResults", getConsumeResultMetrics());
        metrics.put("providerHealth", getProviderHealthMetrics());

        return metrics;
    }

    // ==================== 新增延迟指标方法 ====================

    /**
     * 记录发送延迟
     * @param topic 主题
     * @param latencyMs 延迟时间（毫秒）
     */
    public void recordSendLatency(String topic, long latencyMs) {
        LatencyMetrics latency = sendLatencyMetrics.computeIfAbsent(topic, k -> new LatencyMetrics());
        latency.record(latencyMs);
    }

    /**
     * 记录消费延迟
     * @param topic 主题
     * @param latencyMs 延迟时间（毫秒）
     */
    public void recordConsumeLatency(String topic, long latencyMs) {
        LatencyMetrics latency = consumeLatencyMetrics.computeIfAbsent(topic, k -> new LatencyMetrics());
        latency.record(latencyMs);
    }

    /**
     * 获取发送延迟指标
     */
    public Map<String, Object> getSendLatencyMetrics() {
        Map<String, Object> result = new java.util.HashMap<>();
        for (Map.Entry<String, LatencyMetrics> entry : sendLatencyMetrics.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getStats());
        }
        return result;
    }

    /**
     * 获取消费延迟指标
     */
    public Map<String, Object> getConsumeLatencyMetrics() {
        Map<String, Object> result = new java.util.HashMap<>();
        for (Map.Entry<String, LatencyMetrics> entry : consumeLatencyMetrics.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getStats());
        }
        return result;
    }

    /**
     * 记录批量发送
     * @param topic 主题
     * @param successCount 成功数量
     * @param failedCount 失败数量
     */
    public void recordBatchSent(String topic, int successCount, int failedCount) {
        batchMetrics.computeIfAbsent(topic + ":batch_success", k -> new java.util.concurrent.atomic.AtomicLong(0)).addAndGet(successCount);
        batchMetrics.computeIfAbsent(topic + ":batch_failed", k -> new java.util.concurrent.atomic.AtomicLong(0)).addAndGet(failedCount);
        batchMetrics.computeIfAbsent(topic + ":batch_total", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    /**
     * 获取批量操作指标
     */
    public Map<String, Long> getBatchMetrics() {
        Map<String, Long> result = new java.util.HashMap<>();
        for (Map.Entry<String, java.util.concurrent.atomic.AtomicLong> entry : batchMetrics.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    // ==================== 新增消费结果指标方法 ====================

    /**
     * 记录消费成功
     * @param topic 主题
     */
    public void recordConsumeSuccess(String topic) {
        consumeResultMetrics.computeIfAbsent(topic + ":consume_success", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    /**
     * 记录消费失败
     * @param topic 主题
     */
    public void recordConsumeFailed(String topic) {
        consumeResultMetrics.computeIfAbsent(topic + ":consume_failed", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    /**
     * 记录消费重试
     * @param topic 主题
     */
    public void recordConsumeRetry(String topic) {
        consumeResultMetrics.computeIfAbsent(topic + ":consume_retry", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    /**
     * 记录消费暂停
     * @param topic 主题
     */
    public void recordConsumeSuspend(String topic) {
        consumeResultMetrics.computeIfAbsent(topic + ":consume_suspend", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
    }

    /**
     * 获取消费结果指标
     */
    public Map<String, Long> getConsumeResultMetrics() {
        Map<String, Long> result = new java.util.HashMap<>();
        for (Map.Entry<String, java.util.concurrent.atomic.AtomicLong> entry : consumeResultMetrics.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    // ==================== 新增提供者健康指标方法 ====================

    /**
     * 记录提供者健康状态
     * @param providerName 提供者名称
     */
    public void recordProviderHealthy(String providerName) {
        providerHealthMetrics.computeIfAbsent(providerName + ":healthy_checks", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
        providerHealthMetrics.put(providerName + ":last_healthy_time", new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis()));
    }

    /**
     * 记录提供者不健康状态
     * @param providerName 提供者名称
     */
    public void recordProviderUnhealthy(String providerName) {
        providerHealthMetrics.computeIfAbsent(providerName + ":unhealthy_checks", k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
        providerHealthMetrics.put(providerName + ":last_unhealthy_time", new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis()));
    }

    /**
     * 获取提供者健康指标
     */
    public Map<String, Long> getProviderHealthMetrics() {
        Map<String, Long> result = new java.util.HashMap<>();
        for (Map.Entry<String, java.util.concurrent.atomic.AtomicLong> entry : providerHealthMetrics.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    // ==================== 新增统计方法 ====================

    /**
     * 获取主题的详细统计信息
     * @param topic 主题
     * @return 统计信息
     */
    public Map<String, Object> getTopicStats(String topic) {
        Map<String, Object> stats = new java.util.HashMap<>();

        // 基础指标
        stats.put("sent", topicMetrics.getOrDefault(topic + ":sent", new java.util.concurrent.atomic.AtomicLong(0)).get());
        stats.put("received", topicMetrics.getOrDefault(topic + ":received", new java.util.concurrent.atomic.AtomicLong(0)).get());
        stats.put("failed", topicMetrics.getOrDefault(topic + ":failed", new java.util.concurrent.atomic.AtomicLong(0)).get());

        // 延迟指标
        LatencyMetrics sendLatency = sendLatencyMetrics.get(topic);
        if (sendLatency != null) {
            stats.put("sendLatency", sendLatency.getStats());
        }

        LatencyMetrics consumeLatency = consumeLatencyMetrics.get(topic);
        if (consumeLatency != null) {
            stats.put("consumeLatency", consumeLatency.getStats());
        }

        // 消费结果指标
        stats.put("consumeSuccess", consumeResultMetrics.getOrDefault(topic + ":consume_success", new java.util.concurrent.atomic.AtomicLong(0)).get());
        stats.put("consumeFailed", consumeResultMetrics.getOrDefault(topic + ":consume_failed", new java.util.concurrent.atomic.AtomicLong(0)).get());
        stats.put("consumeRetry", consumeResultMetrics.getOrDefault(topic + ":consume_retry", new java.util.concurrent.atomic.AtomicLong(0)).get());

        return stats;
    }

    /**
     * 重置所有指标
     */
    public void reset() {
        totalSentMessages.set(0);
        totalReceivedMessages.set(0);
        totalFailedMessages.set(0);
        topicMetrics.clear();
        sendLatencyMetrics.clear();
        consumeLatencyMetrics.clear();
        batchMetrics.clear();
        consumeResultMetrics.clear();
        providerHealthMetrics.clear();
    }

    /**
     * 重置指定主题的指标
     * @param topic 主题
     */
    public void resetTopic(String topic) {
        topicMetrics.entrySet().removeIf(entry -> entry.getKey().startsWith(topic + ":"));
        sendLatencyMetrics.remove(topic);
        consumeLatencyMetrics.remove(topic);
        batchMetrics.entrySet().removeIf(entry -> entry.getKey().startsWith(topic + ":"));
        consumeResultMetrics.entrySet().removeIf(entry -> entry.getKey().startsWith(topic + ":"));
    }
}
