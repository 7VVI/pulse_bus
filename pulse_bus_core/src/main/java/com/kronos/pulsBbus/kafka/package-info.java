/**
 * @author zhangyh
 * @Date 2025/6/3 9:30
 * @desc Kafka消息队列提供者包
 * 
 * 配置示例:
 * message:
 *   queue:
 *     default-provider: kafka
 *     providers:
 *       kafka:
 *         name: kafka
 *         is-enable: true
 *         bootstrap-servers: localhost:9092
 *         group-id: pulse-bus-group
 *         client-id: pulse-bus-client
 *         producer:
 *           retries: 3
 *           batch-size: 16384
 *           linger-ms: 1
 *           buffer-memory: 33554432
 *           key-serializer: org.apache.kafka.common.serialization.StringSerializer
 *           value-serializer: org.apache.kafka.common.serialization.StringSerializer
 *           acks: all
 *           enable-idempotence: true
 *           max-in-flight-requests-per-connection: 5
 *         consumer:
 *           auto-offset-reset: earliest
 *           enable-auto-commit: false
 *           max-poll-records: 500
 *           max-poll-interval-ms: 300000
 *           session-timeout-ms: 30000
 *           heartbeat-interval-ms: 3000
 *           key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
 *           value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
 *         batch:
 *           enabled: true
 *           batch-size: 100
 *           batch-timeout-ms: 1000
 *           max-retry-count: 3
 *         retry:
 *           max-retry-count: 3
 *           retry-delay-ms: 1000
 *           retry-multiplier: 2.0
 *           max-retry-delay-ms: 30000
 * 
 * 使用方式:
 * 1. 在application.yml中配置Kafka相关参数
 * 2. 设置message.queue.default-provider=kafka
 * 3. 注入MessageQueueTemplate或MessageQueueManager使用
 * 
 * 特性:
 * - 支持同步和异步消息发送
 * - 支持批量消息发送
 * - 支持消息消费和订阅
 * - 支持手动提交偏移量
 * - 支持消费者组管理
 * - 支持生产者幂等性
 * - 集成监控指标收集
 */
package com.kronos.pulsBbus.kafka;