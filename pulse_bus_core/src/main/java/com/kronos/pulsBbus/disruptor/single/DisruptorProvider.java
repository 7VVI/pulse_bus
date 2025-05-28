package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.MessageQueueProvider;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhangyh
 * @Date 2025/5/28 19:37
 * @desc
 */
public class DisruptorProvider  implements MessageQueueProvider {
    private final Map<String, TopicDisruptor>       topicDisruptors = new ConcurrentHashMap<>();
    private final Map<String, Set<MessageConsumer>> topicConsumers  = new ConcurrentHashMap<>();
    private final ExecutorService                   executorService;
    private final AtomicBoolean                     initialized     = new AtomicBoolean(false);
    private final AtomicBoolean                     healthy         = new AtomicBoolean(false);

    // 配置参数
    private int bufferSize = 1024;
    private String waitStrategy = "BlockingWaitStrategy";
    private int consumerThreads = 1;
    private boolean             enableMetrics = true;
    private MessageQueueMetrics metrics;

    public DisruptorProvider() {
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "disruptor-consumer-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public String getProviderName() {
        return "disruptor";
    }

    @Override
    public MessageQueueTemplate getTemplate() {
        return new DisruptorTemplate(this);
    }

    @Override
    public void initialize(Map<String, Object> config) {
        if (initialized.compareAndSet(false, true)) {
            configureFromMap(config);
            healthy.set(true);
        }
    }

    @Override
    public boolean isHealthy() {
        return healthy.get() && !executorService.isShutdown();
    }

    @Override
    public void destroy() {
        healthy.set(false);

        // 关闭所有Disruptor实例
        for (TopicDisruptor topicDisruptor : topicDisruptors.values()) {
            topicDisruptor.shutdown();
        }
        topicDisruptors.clear();
        topicConsumers.clear();

        // 关闭执行器
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    /**
     * 从配置映射中解析配置
     */
    private void configureFromMap(Map<String, Object> config) {
        if (config != null) {
            bufferSize = (Integer) config.getOrDefault("bufferSize", 1024);
            waitStrategy = (String) config.getOrDefault("waitStrategy", "BlockingWaitStrategy");
            consumerThreads = (Integer) config.getOrDefault("consumerThreads", 1);
            enableMetrics = (Boolean) config.getOrDefault("enableMetrics", true);
        }
    }

    /**
     * 获取或创建主题对应的Disruptor实例
     */
    public TopicDisruptor getOrCreateTopicDisruptor(String topic) {
        return topicDisruptors.computeIfAbsent(topic, t -> {
            WaitStrategy strategy = createWaitStrategy();
            Disruptor<DisruptorMessageEvent> disruptor = new Disruptor<>(
                    new DisruptorMessageEventFactory(),
                    bufferSize,
                    Executors.defaultThreadFactory(),
                    ProducerType.MULTI,
                    strategy
            );

            TopicDisruptor topicDisruptor = new TopicDisruptor(topic, disruptor);
            topicDisruptor.start();
            return topicDisruptor;
        });
    }

    /**
     * 创建等待策略
     */
    private WaitStrategy createWaitStrategy() {
        switch (waitStrategy.toLowerCase()) {
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

    /**
     * 发送消息
     */
    public SendResult sendMessage(String topic, DisruptorMessageEvent message) {
        try {
            if (!healthy.get()) {
                return new SendResult(false, null, "Provider is not healthy");
            }

            TopicDisruptor topicDisruptor = getOrCreateTopicDisruptor(topic);
            return topicDisruptor.publishMessage(message);
        } catch (Exception e) {
            return new SendResult(false, null, "Send failed: " + e.getMessage());
        }
    }

    /**
     * 订阅消息
     */
    public void subscribeMessage(String topic, MessageConsumer consumer) {
        topicConsumers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(consumer);

        // 确保Disruptor已启动
        getOrCreateTopicDisruptor(topic);
    }

    /**
     * 取消订阅
     */
    public void unsubscribeMessage(String topic, MessageConsumer consumer) {
        Set<MessageConsumer> consumers = topicConsumers.get(topic);
        if (consumers != null) {
            consumers.remove(consumer);
            if (consumers.isEmpty()) {
                topicConsumers.remove(topic);
                // 可选：关闭空的Disruptor实例
                TopicDisruptor topicDisruptor = topicDisruptors.remove(topic);
                if (topicDisruptor != null) {
                    topicDisruptor.shutdown();
                }
            }
        }
    }

    /**
     * 获取主题的消费者列表
     */
    public Set<MessageConsumer> getTopicConsumers(String topic) {
        return topicConsumers.getOrDefault(topic, Collections.emptySet());
    }
}
