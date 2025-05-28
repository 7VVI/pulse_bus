package com.kronos.pulsBbus.disruptor.single;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.single.MessageConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:39
 * @desc
 */
public class DisruptorMessageEventHandler implements com.lmax.disruptor.EventHandler<DisruptorMessageEvent> {

    private final MessageQueueMetrics metrics;

    private final Map<String, MessageConsumer> consumers = new ConcurrentHashMap<>();

    public DisruptorMessageEventHandler() {
        this.metrics = new MessageQueueMetrics();
    }

    /**
     * 注册消费者
     */
    public void registerConsumer(String topic, MessageConsumer consumer) {
        consumers.put(topic, consumer);
    }

    /**
     * 移除消费者
     */
    public void removeConsumer(String topic) {
        consumers.remove(topic);
    }

    @Override
    public void onEvent(DisruptorMessageEvent event, long sequence, boolean endOfBatch) throws Exception {
        long startTime = System.currentTimeMillis();
        String topic = event.getTopic();
        MessageConsumer consumer = consumers.get(topic);
        if (consumer != null) {
            try {
                // 转换为标准Message对象
                Message message = convertToMessage(event);
                ConsumeResult result = consumer.consume(message);
                String topicName = event.getTopic();
                // 记录消费指标
                metrics.recordReceivedMessage(topicName);
                metrics.recordConsumeLatency(topicName, System.currentTimeMillis() - startTime);

                switch (result) {
                    case SUCCESS:
                        metrics.recordConsumeSuccess(topicName);
                        break;
                    case RETRY:
                        metrics.recordConsumeRetry(topicName);
                        break;
                    case FAILED:
                        metrics.recordConsumeFailed(topicName);
                        break;
                    case SUSPEND:
                        metrics.recordConsumeSuspend(topicName);
                        break;
                }


            } catch (Exception e) {
                System.err.println("Disruptor事件处理异常: " + e.getMessage());
            }
        }
    }

    /**
     * 转换事件为消息
     */
    private Message convertToMessage(DisruptorMessageEvent event) {
        Message message = new Message();
        message.setId(event.getEventId());
        message.setTopic(event.getTopic());
        message.setPayload(event.getPayload());
        message.setProperties(event.getProperties());
        message.setTimestamp(java.time.LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(event.getTimestamp()),
                java.time.ZoneId.systemDefault()
        ));
        return message;
    }
}
