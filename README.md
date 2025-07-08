# pulse_bus
“pulse 脉冲”，代表快速、连续、高频的消息传递，体现你的消息队列“实时性强、响应快”的特性。

## 核心优化特性

### 1. **统一接口设计**

- `MessageQueueTemplate` 提供统一的消息发送/接收接口
- 完全屏蔽不同MQ中间件的API差异
- 支持同步、异步、批量、延迟、事务等多种消息模式

### 2. **插件化架构**

- `MessageQueueProvider` 插件接口，支持多种MQ中间件
- 适配器模式实现不同MQ的无缝切换
- 已实现RabbitMQ、Redis适配器，可扩展Kafka、RocketMQ等

### 3. **高性能特性**

- **批量处理**: `sendBatch()` 方法支持批量发送消息
- **异步处理**: `sendAsync()` 返回CompletableFuture
- **连接池**: 内置线程池管理异步任务
- **延迟消息**: 支持延迟消息发送功能

### 4. **监控友好**

- **健康检查**: `/actuator/mq/health` 端点监控各提供者状态

- **度量收集**: `MessageQueueMetrics` 统计发送/接收/失败消息数量

- **实时监控**: `/actuator/mq/metrics` 端点暴露实时指标

- ### 1. **延迟指标**

  - `recordSendLatency()`: 记录发送延迟
  - `recordConsumeLatency()`: 记录消费延迟
  - 支持平均值、最小值、最大值、P50/P95/P99百分位数

  ### 2. **批量操作指标**

  - `recordBatchSent()`: 记录批量发送结果
  - 统计批量操作的成功/失败数量

  ### 3. **消费结果指标**

  - `recordConsumeSuccess()`: 记录消费成功
  - `recordConsumeFailed()`: 记录消费失败
  - `recordConsumeRetry()`: 记录消费重试
  - `recordConsumeSuspend()`: 记录消费暂停

  ### 4. **提供者健康指标**

  - `recordProviderHealthy()`: 记录提供者健康状态
  - `recordProviderUnhealthy()`: 记录提供者不健康状态

  ### 5. **增强的统计功能**

  - `getTopicStats()`: 获取单个主题的详细统计
  - `reset()`: 重置所有指标
  - `resetTopic()`: 重置指定主题的指标

### 5. **配置灵活**

- **多提供者配置**: 支持同时配置多个MQ提供者
- **动态切换**: 运行时可切换不同的MQ提供者
- **重试策略**: 可配置重试次数、延迟、退避算法
- **批量配置**: 可配置批次大小、超时时间等

## 架构设计优势

### 核心设计模式

- **适配器模式**: 统一不同MQ中间件的接口
- **策略模式**: 可插拔的提供者实现
- **模板方法模式**: 统一的消息处理流程
- **观察者模式**: 消息订阅和消费机制

### 扩展性

- 新增MQ支持只需实现`MessageQueueProvider`接口
- 配置驱动，无需修改代码即可切换MQ
- 支持自定义消息序列化策略
- 支持自定义重试和容错策略

### 企业级特性

- **高可用**: 支持多提供者故障切换
- **监控集成**: 与Spring Boot Actuator深度集成
- **配置管理**: 支持外部化配置和动态配置
- **事务支持**: 提供事务消息发送能力

## 使用方式

1. **添加依赖** - 引入SDK依赖包
2. **配置提供者** - 在application.yml中配置MQ连接信息
3. **注入模板** - 使用`MessageQueueTemplate`发送消息
4. **监控集成** - 通过Actuator端点监控消息队列状态

这个架onstrukdal形成了一个完整的企业级消息队列中间件SDK，既保持了简单易用的特性，又具备了生产环境所需的高性能、高可用、可监控等特性。
