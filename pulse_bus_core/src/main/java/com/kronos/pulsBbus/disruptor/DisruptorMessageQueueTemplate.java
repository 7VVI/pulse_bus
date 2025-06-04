package com.kronos.pulsBbus.disruptor;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchMessageConsumer;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;
import com.kronos.pulsBbus.disruptor.batch.DisruptorBatchEvent;
import com.kronos.pulsBbus.disruptor.batch.DisruptorBatchEventFactory;
import com.kronos.pulsBbus.disruptor.batch.DisruptorBatchEventHandler;
import com.kronos.pulsBbus.disruptor.single.DisruptorMessageEvent;
import com.kronos.pulsBbus.disruptor.single.DisruptorMessageEventFactory;
import com.kronos.pulsBbus.disruptor.single.DisruptorMessageEventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:46
 * @desc
 */
@org.springframework.stereotype.Component
public class DisruptorMessageQueueTemplate implements MessageQueueTemplate {

    private static final Logger              log = LoggerFactory.getLogger(DisruptorMessageQueueTemplate.class);
    private final        DisruptorProperties properties;
    private final MessageQueueMetrics   metrics;
    private final DisruptorRetryHandler retryHandler;

    // 单消息处理相关
    private final java.util.Map<String, RingBuffer<DisruptorMessageEvent>> messageRingBuffers = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, Disruptor<DisruptorMessageEvent>>  messageDisruptors  = new java.util.concurrent.ConcurrentHashMap<>();

    // 批量消息处理相关
    private final java.util.Map<String, RingBuffer<DisruptorBatchEvent>> batchRingBuffers = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, Disruptor<DisruptorBatchEvent>> batchDisruptors = new java.util.concurrent.ConcurrentHashMap<>();

    // 线程工厂
    private final java.util.concurrent.ThreadFactory threadFactory;

    public DisruptorMessageQueueTemplate(DisruptorProperties properties,
                                         MessageQueueMetrics metrics,
                                         DisruptorRetryHandler retryHandler) {
        this.properties = properties;
        this.metrics = metrics;
        this.retryHandler = retryHandler;

        // 创建线程工厂
        this.threadFactory = new java.util.concurrent.ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger threadNumber = new java.util.concurrent.atomic.AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Disruptor-Consumer-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
    }

    @Override
    public SendResult send(String topic, Object message) {
        long startTime = System.currentTimeMillis();

        try {
            RingBuffer<DisruptorMessageEvent> ringBuffer = getOrCreateMessageRingBuffer(topic);

            // 创建消息对象
            Message msg = createMessage(topic, message);

            // 发布到RingBuffer
            long sequence = ringBuffer.next();
            try {
                DisruptorMessageEvent event = ringBuffer.get(sequence);
                event.set(topic, msg);
            } finally {
                ringBuffer.publish(sequence);
            }

            // 记录指标
            if (metrics != null) {
                metrics.recordSentMessage(topic);
                metrics.recordSendLatency(topic, System.currentTimeMillis() - startTime);
            }

            return new SendResult(true, msg.getId(), "消息发送成功");

        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFailedMessage(topic);
            }
            return new SendResult(false, null, "消息发送失败: " + e.getMessage());
        }
    }

    @Override
    public java.util.concurrent.CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> send(topic, message));
    }

    @Override
    public BatchSendResult sendBatch(String topic, java.util.List<Object> messages) {
        BatchSendResult result = new BatchSendResult(messages.size());

        for (Object message : messages) {
            SendResult sendResult = send(topic, message);
            result.addResult(sendResult);
        }

        return result;
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        java.util.concurrent.ScheduledExecutorService scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        scheduler.schedule(() -> {
            send(topic, message);
            scheduler.shutdown();
        }, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);

        Message msg = createMessage(topic, message);
        return new SendResult(true, msg.getId(), "延迟消息已调度");
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        try {
            callback.doInTransaction();
            return send(topic, message);
        } catch (Exception e) {
            return new SendResult(false, null, "事务执行失败: " + e.getMessage());
        }
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        // 创建单消息处理的Disruptor
        int ringBufferSize = DisruptorUtils.nextPowerOfTwo(properties.getRingBufferSize());
        WaitStrategy waitStrategy = DisruptorUtils.createWaitStrategy(properties.getWaitStrategy());
        ProducerType producerType = DisruptorUtils.createProducerType(properties.getProducerType());

        Disruptor<DisruptorMessageEvent> disruptor = new Disruptor<>(
                new DisruptorMessageEventFactory(),
                ringBufferSize,
                threadFactory,
                producerType,
                waitStrategy
        );

        // 设置事件处理器
        DisruptorMessageEventHandler handler = new DisruptorMessageEventHandler(topic, consumer, metrics, retryHandler);
        disruptor.handleEventsWith(handler);

        // 启动Disruptor
        disruptor.start();
        RingBuffer<DisruptorMessageEvent> ringBuffer = disruptor.getRingBuffer();

        // 保存引用
        messageDisruptors.put(topic, disruptor);
        messageRingBuffers.put(topic, ringBuffer);
        log.info("Disruptor订阅成功 - Topic: {}, RingBuffer大小: {}", topic, ringBufferSize);
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        // 停止单消息Disruptor
        Disruptor<DisruptorMessageEvent> disruptor = messageDisruptors.remove(topic);
        if (disruptor != null) {
            disruptor.shutdown();
        }
        messageRingBuffers.remove(topic);
        log.info("Disruptor取消订阅成功 - Topic: {}", topic);
    }

    /**
     * 订阅批量消息
     */
    public void subscribeBatch(String topic, BatchMessageConsumer consumer) {
        if (!properties.getBatch().isEnabled()) {
            throw new IllegalStateException("批量消费未启用");
        }

        // 创建批量处理的Disruptor
        int ringBufferSize = DisruptorUtils.nextPowerOfTwo(properties.getRingBufferSize());
        WaitStrategy waitStrategy = DisruptorUtils.createWaitStrategy(properties.getWaitStrategy());
        ProducerType producerType = DisruptorUtils.createProducerType(properties.getProducerType());

        Disruptor<DisruptorBatchEvent> batchDisruptor = new Disruptor<>(
                new DisruptorBatchEventFactory(),
                ringBufferSize,
                threadFactory,
                producerType,
                waitStrategy
        );

        // 设置批量事件处理器
        DisruptorBatchEventHandler batchHandler = new DisruptorBatchEventHandler(topic, consumer, metrics, retryHandler);
        batchDisruptor.handleEventsWith(batchHandler);

        // 启动批量Disruptor
        batchDisruptor.start();
        RingBuffer<DisruptorBatchEvent> batchRingBuffer = batchDisruptor.getRingBuffer();

        // 保存引用
        batchDisruptors.put(topic, batchDisruptor);
        batchRingBuffers.put(topic, batchRingBuffer);
         log.info("Disruptor批量订阅成功 - Topic: {}, 批次大小: {}, 超时时间: {}ms", topic, properties.getBatch().getBatchSize(), properties.getBatch().getBatchTimeoutMs());
    }

    /**
     * 取消批量订阅
     */
    public void unsubscribeBatch(String topic) {
        Disruptor<DisruptorBatchEvent> batchDisruptor = batchDisruptors.remove(topic);
        if (batchDisruptor != null) {
            batchDisruptor.shutdown();
        }
        batchRingBuffers.remove(topic);
        log.info("Disruptor批量取消订阅成功 - Topic: {}", topic);
    }

    /**
     * 发送批量消息到批量处理器
     */
    public void sendBatchToBatchProcessor(String topic, java.util.List<Message> messages) {
        RingBuffer<DisruptorBatchEvent> batchRingBuffer = batchRingBuffers.get(topic);
        if (batchRingBuffer != null && !messages.isEmpty()) {
            long sequence = batchRingBuffer.next();
            try {
                DisruptorBatchEvent event = batchRingBuffer.get(sequence);
                event.setBatch(topic, messages);
            } finally {
                batchRingBuffer.publish(sequence);
            }
        }
    }

    /**
     * 获取或创建消息RingBuffer
     */
    private RingBuffer<DisruptorMessageEvent> getOrCreateMessageRingBuffer(String topic) {
        return messageRingBuffers.computeIfAbsent(topic, t -> {
            // 创建临时的Disruptor用于发送
            int ringBufferSize = DisruptorUtils.nextPowerOfTwo(properties.getRingBufferSize());
            WaitStrategy waitStrategy = DisruptorUtils.createWaitStrategy(properties.getWaitStrategy());
            ProducerType producerType = DisruptorUtils.createProducerType(properties.getProducerType());

            Disruptor<DisruptorMessageEvent> disruptor = new Disruptor<>(
                    new DisruptorMessageEventFactory(),
                    ringBufferSize,
                    threadFactory,
                    producerType,
                    waitStrategy
            );

            // 设置默认处理器（丢弃消息）
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                log.info("消息被丢弃，没有订阅者 - Topic: {}, MessageId: {}", event.getTopic(), event.getMessage().getId());
                event.setProcessed(true);
            });

            disruptor.start();
            messageDisruptors.put(t, disruptor);
            return disruptor.getRingBuffer();
        });
    }

    /**
     * 创建消息对象
     */
    private Message createMessage(String topic, Object payload) {
        Message message = new Message();
        message.setId(java.util.UUID.randomUUID().toString());
        message.setTopic(topic);
        message.setPayload(payload);
        message.setTimestamp(LocalDateTime.now());
        message.setRetryCount(0);
        return message;
    }

    /**
     * 获取统计信息
     */
    public java.util.Map<String, Object> getStats() {
        java.util.Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("activeTopics", messageRingBuffers.size());
        stats.put("activeBatchTopics", batchRingBuffers.size());
        stats.put("ringBufferSize", properties.getRingBufferSize());
        stats.put("waitStrategy", properties.getWaitStrategy());
        stats.put("producerType", properties.getProducerType());

        // 单消息统计
        java.util.Map<String, Object> messageStats = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, RingBuffer<DisruptorMessageEvent>> entry : messageRingBuffers.entrySet()) {
            String topic = entry.getKey();
            RingBuffer<DisruptorMessageEvent> ringBuffer = entry.getValue();

            java.util.Map<String, Object> topicStat = new java.util.HashMap<>();
            topicStat.put("bufferSize", ringBuffer.getBufferSize());
            topicStat.put("remainingCapacity", ringBuffer.remainingCapacity());
            topicStat.put("cursor", ringBuffer.getCursor());

            messageStats.put(topic, topicStat);
        }
        stats.put("messageTopics", messageStats);

        // 批量消息统计
        java.util.Map<String, Object> batchStats = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, RingBuffer<DisruptorBatchEvent>> entry : batchRingBuffers.entrySet()) {
            String topic = entry.getKey();
            RingBuffer<DisruptorBatchEvent> ringBuffer = entry.getValue();

            java.util.Map<String, Object> topicStat = new java.util.HashMap<>();
            topicStat.put("bufferSize", ringBuffer.getBufferSize());
            topicStat.put("remainingCapacity", ringBuffer.remainingCapacity());
            topicStat.put("cursor", ringBuffer.getCursor());

            batchStats.put(topic, topicStat);
        }
        stats.put("batchTopics", batchStats);

        return stats;
    }

    /**
     * 关闭模板
     */
    public void shutdown() {
        // 关闭所有单消息Disruptor
        for (Disruptor<DisruptorMessageEvent> disruptor : messageDisruptors.values()) {
            disruptor.shutdown();
        }

        // 关闭所有批量Disruptor
        for (Disruptor<DisruptorBatchEvent> disruptor : batchDisruptors.values()) {
            disruptor.shutdown();
        }
        log.info("DisruptorMessageQueueTemplate已关闭");
    }
}
