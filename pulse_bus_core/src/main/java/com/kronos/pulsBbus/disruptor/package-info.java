/**
 * @author zhangyh
 * @Date 2025/6/3 8:56
 * @desc
 * message:
 *   queue:
 *     provider: disruptor
 *     disruptor:
 *       # RingBuffer大小，必须是2的幂
 *       ring-buffer-size: 1024
 *       # 等待策略: BLOCKING, BUSY_SPIN, YIELDING, SLEEPING, TIMEOUT_BLOCKING
 *       wait-strategy: BLOCKING
 *       # 生产者类型: SINGLE, MULTI
 *       producer-type: MULTI
 *       # 消费者线程池大小
 *       consumer-thread-pool-size: 4
 *       # 批量消费配置
 *       batch:
 *         enabled: true
 *         batch-size: 100
 *         batch-timeout-ms: 1000
 *         max-retry-count: 3
 *       # 重试配置
 *       retry:
 *         max-retry-count: 3
 *         retry-delay-ms: 1000
 *         retry-multiplier: 2.0
 *         max-retry-delay-ms: 30000
 */
package com.kronos.pulsBbus.disruptor;