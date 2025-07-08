package com.kronos.pulsBbus.kafka;

import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.TransactionCallback;
import com.kronos.pulsBbus.core.batch.BatchSendResult;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.single.MessageConsumer;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import com.kronos.pulsBbus.core.single.SendResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author zhangyh
 * @Date 2025/6/3 9:10
 * @desc Kafka消息队列模板实现
 */
public class KafkaMessageQueueTemplate implements MessageQueueTemplate {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageQueueTemplate.class);

    private final KafkaProperties kafkaConfig;
    private final MessageQueueMetrics metrics;
    private final KafkaProducer<String, String> producer;
    private final Map<String, KafkaConsumer<String, String>> consumers = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService> consumerExecutors = new ConcurrentHashMap<>();
    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();
    private volatile boolean shutdown = false;

    public KafkaMessageQueueTemplate(KafkaProperties properties, MessageQueueMetrics metrics) {
        this.kafkaConfig = properties;
        this.metrics = metrics;
        this.producer = createProducer();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getBootstrapServers());
        props.put("client.id", kafkaConfig.getClientId());
        props.put("key.serializer", kafkaConfig.getProducer().getKeySerializer());
        props.put("value.serializer", kafkaConfig.getProducer().getValueSerializer());
        props.put("acks", kafkaConfig.getProducer().getAcks());
        props.put("retries", kafkaConfig.getProducer().getRetries());
        props.put("batch.size", kafkaConfig.getProducer().getBatchSize());
        props.put("linger.ms", kafkaConfig.getProducer().getLingerMs());
        props.put("buffer.memory", kafkaConfig.getProducer().getBufferMemory());
        props.put("enable.idempotence", kafkaConfig.getProducer().isEnableIdempotence());
        props.put("max.in.flight.requests.per.connection", kafkaConfig.getProducer().getMaxInFlightRequestsPerConnection());

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getBootstrapServers());
        props.put("group.id", kafkaConfig.getGroupId());
        props.put("client.id", kafkaConfig.getClientId() + "-consumer-" + topic);
        props.put("key.deserializer", kafkaConfig.getConsumer().getKeyDeserializer());
        props.put("value.deserializer", kafkaConfig.getConsumer().getValueDeserializer());
        props.put("auto.offset.reset", kafkaConfig.getConsumer().getAutoOffsetReset());
        props.put("enable.auto.commit", kafkaConfig.getConsumer().isEnableAutoCommit());
        props.put("max.poll.records", kafkaConfig.getConsumer().getMaxPollRecords());
        props.put("max.poll.interval.ms", kafkaConfig.getConsumer().getMaxPollIntervalMs());
        props.put("session.timeout.ms", kafkaConfig.getConsumer().getSessionTimeoutMs());
        props.put("heartbeat.interval.ms", kafkaConfig.getConsumer().getHeartbeatIntervalMs());

        return new KafkaConsumer<>(props);
    }

    @Override
    public SendResult send(String topic, Object message) {
        long startTime = System.currentTimeMillis();

        try {
            Message msg = createMessage(topic, message);
            String messageJson = convertToJson(msg);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg.getId(), messageJson);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // 同步发送

            // 记录指标
            if (metrics != null) {
                metrics.recordSentMessage(topic);
                metrics.recordSendLatency(topic, System.currentTimeMillis() - startTime);
            }

            log.debug("消息发送成功 - Topic: {}, Partition: {}, Offset: {}, MessageId: {}", 
                    topic, metadata.partition(), metadata.offset(), msg.getId());

            return new SendResult(true, msg.getId(), "消息发送成功");

        } catch (Exception e) {
            if (metrics != null) {
                metrics.recordFailedMessage(topic);
            }
            log.error("消息发送失败 - Topic: {}, Error: {}", topic, e.getMessage(), e);
            return new SendResult(false, null, "消息发送失败: " + e.getMessage());
        }
    }

    @Override
    public CompletableFuture<SendResult> sendAsync(String topic, Object message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Message msg = createMessage(topic, message);
                String messageJson = convertToJson(msg);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg.getId(), messageJson);
                
                CompletableFuture<SendResult> future = new CompletableFuture<>();
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        if (metrics != null) {
                            metrics.recordSentMessage(topic);
                        }
                        future.complete(new SendResult(true, msg.getId(), "异步消息发送成功"));
                    } else {
                        if (metrics != null) {
                            metrics.recordFailedMessage(topic);
                        }
                        future.complete(new SendResult(false, null, "异步消息发送失败: " + exception.getMessage()));
                    }
                });
                
                return future.get();
            } catch (Exception e) {
                if (metrics != null) {
                    metrics.recordFailedMessage(topic);
                }
                return new SendResult(false, null, "异步消息发送失败: " + e.getMessage());
            }
        }, asyncExecutor);
    }

    @Override
    public BatchSendResult sendBatch(String topic, List<Object> messages) {
        BatchSendResult result = new BatchSendResult(messages.size());

        for (Object message : messages) {
            SendResult sendResult = send(topic, message);
            result.addResult(sendResult);
        }

        return result;
    }

    @Override
    public SendResult sendDelay(String topic, Object message, long delayMs) {
        // Kafka本身不支持延迟消息，这里使用调度器实现
        java.util.concurrent.ScheduledExecutorService scheduler = 
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        scheduler.schedule(() -> {
            send(topic, message);
            scheduler.shutdown();
        }, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);

        Message msg = createMessage(topic, message);
        return new SendResult(true, msg.getId(), "延迟消息已调度");
    }

    @Override
    public SendResult sendTransaction(String topic, Object message, TransactionCallback callback) {
        // Kafka事务支持需要特殊配置，这里简化实现
        try {
            callback.doInTransaction();
            return send(topic, message);
        } catch (Exception e) {
            return new SendResult(false, null, "事务执行失败: " + e.getMessage());
        }
    }

    @Override
    public void subscribe(String topic, MessageConsumer consumer) {
        if (shutdown) {
            throw new IllegalStateException("Kafka模板已关闭");
        }

        KafkaConsumer<String, String> kafkaConsumer = createConsumer(topic);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        consumers.put(topic, kafkaConsumer);

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "Kafka-Consumer-" + topic);
            t.setDaemon(true);
            return t;
        });
        consumerExecutors.put(topic, executor);

        executor.submit(() -> {
            log.info("Kafka消费者启动 - Topic: {}", topic);
            
            while (!shutdown && !Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            Message message = parseMessage(record.value());
                            message.setTopic(topic);
                            
                            long startTime = System.currentTimeMillis();
                            consumer.consume(message);
                            
                            // 手动提交偏移量
                            kafkaConsumer.commitSync();
                            
                            if (metrics != null) {
                                metrics.recordReceivedMessage(topic);
                                metrics.recordConsumeLatency(topic, System.currentTimeMillis() - startTime);
                            }
                            
                        } catch (Exception e) {
                            log.error("消息消费失败 - Topic: {}, Offset: {}, Error: {}", 
                                    topic, record.offset(), e.getMessage(), e);
                            if (metrics != null) {
                                metrics.recordFailedMessage(topic);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (!shutdown) {
                        log.error("Kafka消费者异常 - Topic: {}, Error: {}", topic, e.getMessage(), e);
                        try {
                            Thread.sleep(1000); // 异常时等待1秒后重试
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            
            log.info("Kafka消费者停止 - Topic: {}", topic);
        });

        log.info("Kafka订阅成功 - Topic: {}", topic);
    }

    @Override
    public void unsubscribe(String topic, String consumerGroup) {
        KafkaConsumer<String, String> consumer = consumers.remove(topic);
        if (consumer != null) {
            consumer.close();
        }

        ExecutorService executor = consumerExecutors.remove(topic);
        if (executor != null) {
            executor.shutdown();
        }

        log.info("Kafka取消订阅成功 - Topic: {}", topic);
    }

    /**
     * 关闭Kafka模板
     */
    public void shutdown() {
        shutdown = true;

        // 关闭所有消费者
        consumers.values().forEach(KafkaConsumer::close);
        consumers.clear();

        // 关闭所有消费者执行器
        consumerExecutors.values().forEach(ExecutorService::shutdown);
        consumerExecutors.clear();

        // 关闭生产者
        if (producer != null) {
            producer.close();
        }

        // 关闭异步执行器
        asyncExecutor.shutdown();

        log.info("Kafka消息队列模板已关闭");
    }

    /**
     * 创建消息对象
     */
    private Message createMessage(String topic, Object payload) {
        return new Message(topic, payload);
    }

    /**
     * 将消息转换为JSON字符串
     */
    private String convertToJson(Message message) {
        try {
            return com.alibaba.fastjson2.JSON.toJSONString(message);
        } catch (Exception e) {
            throw new RuntimeException("消息序列化失败", e);
        }
    }

    /**
     * 解析JSON字符串为消息对象
     */
    private Message parseMessage(String json) {
        try {
            return com.alibaba.fastjson2.JSON.parseObject(json, Message.class);
        } catch (Exception e) {
            throw new RuntimeException("消息反序列化失败", e);
        }
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("activeConsumers", consumers.size());
        stats.put("shutdown", shutdown);
        stats.put("bootstrapServers", kafkaConfig.getBootstrapServers());
        stats.put("groupId", kafkaConfig.getGroupId());
        return stats;
    }
}