package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.SendResult;
import com.kronos.pulsBbus.disruptor.DisruptorConfig;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhangyh
 * @Date 2025/5/28 19:39
 * @desc
 */
public class TopicDisruptor {
    private final String          topic;
    private final DisruptorConfig config;
    private final ExecutorService executor;

    private       Disruptor<DisruptorMessageEvent>  disruptor;
    private       RingBuffer<DisruptorMessageEvent> ringBuffer;
    private final List<MessageConsumer>             consumers         = new CopyOnWriteArrayList<>();
    private final AtomicLong                        sequenceGenerator = new AtomicLong(0);
    private final AtomicBoolean                     started           = new AtomicBoolean(false);

    public TopicDisruptor(String topic, DisruptorConfig config, ExecutorService executor) {
        this.topic = topic;
        this.config = config;
        this.executor = executor;
    }

    @SuppressWarnings("unchecked")
    public void start() {
        if (started.compareAndSet(false, true)) {
            // 创建Disruptor
            disruptor = new Disruptor<>(
                    new DisruptorMessageEventFactory(),
                    config.getBufferSize(),
                    Executors.defaultThreadFactory(),
                    ProducerType.MULTI, // 支持多生产者
                    createWaitStrategy(config.getWaitStrategy())
            );

            // 设置事件处理器
            disruptor.handleEventsWith(new DisruptorMessageEventHandler());

            // 启动Disruptor
            disruptor.start();
            ringBuffer = disruptor.getRingBuffer();
        }
    }

    public boolean isHealthy() {
        return started.get() && disruptor != null && !executor.isShutdown();
    }

    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            if (disruptor != null) {
                disruptor.shutdown();
            }
            consumers.clear();
        }
    }

    public SendResult send(Object message) {
        if (!started.get()) {
            return new SendResult(false, null, "TopicDisruptor not started");
        }

        try {
            long sequence = ringBuffer.next();
            try {
                DisruptorMessageEvent event = ringBuffer.get(sequence);
                event.setMessage(createMessage(message));
                event.setTimestamp(System.currentTimeMillis());
            } finally {
                ringBuffer.publish(sequence);
            }

            String messageId = topic + "-" + sequenceGenerator.incrementAndGet();
            return new SendResult(true, messageId);

        } catch (Exception e) {
            return new SendResult(false, null, "Send failed: " + e.getMessage());
        }
    }

    public BatchSendResult sendBatch(List<Object> messages) {
        List<SendResult> results = new ArrayList<>();
        int successCount = 0;
        int failedCount = 0;

        for (Object msg : messages) {
            SendResult result = send(msg);
            results.add(result);
            if (result.isSuccess()) {
                successCount++;
            } else {
                failedCount++;
            }
        }

        return new BatchSendResult(results, successCount, failedCount);
    }

    public void subscribe(MessageConsumer consumer) {
        consumers.add(consumer);
    }

    public void unsubscribe(String consumerGroup) {
        // 简单实现，实际可以根据consumerGroup进行更精细的控制
        consumers.clear();
    }

    private Message createMessage(Object payload) {
        Message message = new Message(topic, payload);
        message.setId(topic + "-" + sequenceGenerator.incrementAndGet());
        return message;
    }

    private WaitStrategy createWaitStrategy(String strategyName) {
        switch (strategyName.toLowerCase()) {
            case "blockingwaitstrategy":
                return new BlockingWaitStrategy();
            case "sleepingwaitstrategy":
                return new SleepingWaitStrategy();
            case "yieldingwaitstrategy":
                return new YieldingWaitStrategy();
            case "busyspinwaitstrategy":
                return new BusySpinWaitStrategy();
            case "liteblockingwaitstrategy":
                return new LiteBlockingWaitStrategy();
            default:
                return new BlockingWaitStrategy();
        }
    }
}
