/**
 * Redis消息队列实现包
 * 
 * <p>本包提供了基于Redis的消息队列实现，支持多种消息发送模式和高级功能。</p>
 * 
 * <h2>主要特性</h2>
 * <ul>
 *   <li>同步/异步消息发送</li>
 *   <li>批量消息发送</li>
 *   <li>延迟消息支持</li>
 *   <li>事务消息支持</li>
 *   <li>消息订阅和消费</li>
 *   <li>消息重试机制</li>
 *   <li>连接池管理</li>
 *   <li>健康检查</li>
 *   <li>性能监控</li>
 * </ul>
 * 
 * <h2>配置示例</h2>
 * <pre>{@code
 * # application.yml
 * message:
 *   queue:
 *     enabled: true
 *     default-provider: redis
 *     providers:
 *       redis:
 *         enabled: true
 *         name: redis
 *         fail-fast: true
 *         
 *         # Redis连接配置
 *         host: localhost
 *         port: 6379
 *         database: 0
 *         password: 
 *         timeout: 5000
 *         
 *         # 连接池配置
 *         pool:
 *           max-active: 8
 *           max-idle: 8
 *           min-idle: 0
 *           max-wait: 5000
 *           test-on-borrow: true
 *           test-on-return: false
 *           test-while-idle: true
 *           
 *         # 消息配置
 *         message:
 *           topic-prefix: "mq:"
 *           default-ttl: 300000  # 5分钟
 *           
 *         # 批量处理配置
 *         batch:
 *           enabled: true
 *           max-size: 100
 *           flush-interval: 1000
 *           
 *         # 延迟消息配置
 *         delay:
 *           enabled: true
 *           process-interval: 1000  # 延迟消息处理间隔
 *           
 *         # 重试配置
 *         retry:
 *           enabled: true
 *           max-attempts: 3
 *           backoff-multiplier: 2.0
 *           initial-interval: 1000
 *           max-interval: 30000
 *           
 *         # 监控配置
 *         monitoring:
 *           enabled: true
 *           metrics-interval: 30000
 * }</pre>
 * 
 * <h2>使用示例</h2>
 * 
 * <h3>基本消息发送</h3>
 * <pre>{@code
 * @Autowired
 * private MessageQueueTemplate messageQueueTemplate;
 * 
 * // 同步发送
 * SendResult result = messageQueueTemplate.send("user.created", userEvent);
 * if (result.isSuccess()) {
 *     System.out.println("消息发送成功: " + result.getMessageId());
 * }
 * 
 * // 异步发送
 * CompletableFuture<SendResult> future = messageQueueTemplate.sendAsync("user.updated", userEvent);
 * future.thenAccept(result -> {
 *     if (result.isSuccess()) {
 *         System.out.println("异步消息发送成功: " + result.getMessageId());
 *     }
 * });
 * }</pre>
 * 
 * <h3>批量消息发送</h3>
 * <pre>{@code
 * List<Object> messages = Arrays.asList(
 *     new UserEvent("user1", "created"),
 *     new UserEvent("user2", "updated"),
 *     new UserEvent("user3", "deleted")
 * );
 * 
 * BatchSendResult batchResult = messageQueueTemplate.sendBatch("user.events", messages);
 * System.out.println("批量发送结果: 成功=" + batchResult.getSuccessCount() + 
 *                   ", 失败=" + batchResult.getFailedCount());
 * }</pre>
 * 
 * <h3>延迟消息</h3>
 * <pre>{@code
 * // 发送5分钟后执行的延迟消息
 * SendResult delayResult = messageQueueTemplate.sendDelay(
 *     "order.timeout", 
 *     orderTimeoutEvent, 
 *     5 * 60 * 1000  // 5分钟
 * );
 * }</pre>
 * 
 * <h3>事务消息</h3>
 * <pre>{@code
 * SendResult txResult = messageQueueTemplate.sendTransaction(
 *     "payment.processed", 
 *     paymentEvent,
 *     () -> {
 *         // 在事务中执行的业务逻辑
 *         paymentService.updatePaymentStatus(paymentId, "COMPLETED");
 *         orderService.updateOrderStatus(orderId, "PAID");
 *     }
 * );
 * }</pre>
 * 
 * <h3>消息订阅</h3>
 * <pre>{@code
 * // 订阅消息
 * messageQueueTemplate.subscribe("user.created", message -> {
 *     UserEvent event = (UserEvent) message;
 *     System.out.println("处理用户创建事件: " + event.getUserId());
 *     
 *     // 处理业务逻辑
 *     emailService.sendWelcomeEmail(event.getUserId());
 *     statisticsService.incrementUserCount();
 * });
 * }</pre>
 * 
 * <h2>高级功能</h2>
 * 
 * <h3>自定义序列化</h3>
 * <p>Redis实现使用Java默认序列化机制，支持所有实现Serializable接口的对象。</p>
 * 
 * <h3>消息重试</h3>
 * <p>当消息消费失败时，系统会根据配置自动进行重试，支持指数退避策略。</p>
 * 
 * <h3>健康检查</h3>
 * <pre>{@code
 * @Autowired
 * private MessageQueueManager messageQueueManager;
 * 
 * Map<String, Boolean> healthStatus = messageQueueManager.healthCheck();
 * boolean redisHealthy = healthStatus.get("redis");
 * }</pre>
 * 
 * <h3>性能监控</h3>
 * <pre>{@code
 * @Autowired
 * private MessageQueueMetrics metrics;
 * 
 * long totalSent = metrics.getTotalSentMessages();
 * long totalReceived = metrics.getTotalReceivedMessages();
 * long totalFailed = metrics.getTotalFailedMessages();
 * 
 * Map<String, Long> topicMetrics = metrics.getTopicMetrics();
 * }</pre>
 * 
 * <h2>最佳实践</h2>
 * 
 * <h3>Topic命名规范</h3>
 * <ul>
 *   <li>使用点号分隔的层次结构：{@code domain.entity.action}</li>
 *   <li>示例：{@code user.account.created}, {@code order.payment.completed}</li>
 *   <li>避免使用特殊字符和空格</li>
 * </ul>
 * 
 * <h3>消息设计</h3>
 * <ul>
 *   <li>消息对象应该实现Serializable接口</li>
 *   <li>包含必要的元数据（时间戳、版本等）</li>
 *   <li>保持消息结构的向后兼容性</li>
 * </ul>
 * 
 * <h3>错误处理</h3>
 * <ul>
 *   <li>合理配置重试次数和间隔</li>
 *   <li>实现死信队列处理机制</li>
 *   <li>记录详细的错误日志</li>
 * </ul>
 * 
 * <h3>性能优化</h3>
 * <ul>
 *   <li>合理配置连接池大小</li>
 *   <li>使用批量发送减少网络开销</li>
 *   <li>根据业务需求调整TTL设置</li>
 *   <li>监控Redis内存使用情况</li>
 * </ul>
 * 
 * <h2>注意事项</h2>
 * <ul>
 *   <li>Redis消息队列不保证消息的严格顺序</li>
 *   <li>需要确保Redis服务的高可用性</li>
 *   <li>大消息可能影响性能，建议使用引用传递</li>
 *   <li>延迟消息的精度取决于处理间隔配置</li>
 * </ul>
 * 
 * @author zhangyh
 * @version 1.0
 * @since 2025-01-20
 */
package com.kronos.pulsBbus.redis;