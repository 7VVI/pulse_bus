package com.kronos.pulsBbus.core.monitor;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:19
 * @desc
 */
public class LatencyMetrics {

    private final java.util.concurrent.atomic.AtomicLong count = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong totalLatency = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong minLatency = new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    private final java.util.concurrent.atomic.AtomicLong maxLatency = new java.util.concurrent.atomic.AtomicLong(0);

    // 使用滑动窗口记录最近的延迟数据
    private final java.util.concurrent.ConcurrentLinkedQueue<Long> recentLatencies = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private static final int MAX_RECENT_SIZE = 1000;

    /**
     * 记录延迟数据
     * @param latencyMs 延迟时间（毫秒）
     */
    public void record(long latencyMs) {
        count.incrementAndGet();
        totalLatency.addAndGet(latencyMs);

        // 更新最小值
        minLatency.updateAndGet(current -> Math.min(current, latencyMs));

        // 更新最大值
        maxLatency.updateAndGet(current -> Math.max(current, latencyMs));

        // 添加到最近延迟队列
        recentLatencies.offer(latencyMs);
        if (recentLatencies.size() > MAX_RECENT_SIZE) {
            recentLatencies.poll();
        }
    }

    /**
     * 获取统计信息
     * @return 统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        long currentCount = count.get();

        if (currentCount > 0) {
            stats.put("count", currentCount);
            stats.put("totalLatency", totalLatency.get());
            stats.put("avgLatency", totalLatency.get() / (double) currentCount);
            stats.put("minLatency", minLatency.get() == Long.MAX_VALUE ? 0 : minLatency.get());
            stats.put("maxLatency", maxLatency.get());

            // 计算P95、P99延迟
            if (!recentLatencies.isEmpty()) {
                java.util.List<Long> sortedLatencies = new java.util.ArrayList<>(recentLatencies);
                sortedLatencies.sort(Long::compareTo);

                stats.put("p50Latency", getPercentile(sortedLatencies, 0.5));
                stats.put("p95Latency", getPercentile(sortedLatencies, 0.95));
                stats.put("p99Latency", getPercentile(sortedLatencies, 0.99));
            }
        } else {
            stats.put("count", 0);
            stats.put("totalLatency", 0);
            stats.put("avgLatency", 0.0);
            stats.put("minLatency", 0);
            stats.put("maxLatency", 0);
        }

        return stats;
    }

    /**
     * 计算百分位数
     */
    private long getPercentile(java.util.List<Long> sortedList, double percentile) {
        if (sortedList.isEmpty()) {
            return 0;
        }

        int index = (int) Math.ceil(percentile * sortedList.size()) - 1;
        index = Math.max(0, Math.min(index, sortedList.size() - 1));
        return sortedList.get(index);
    }
}
