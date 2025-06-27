package com.kronos.pulsBbus;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;
import com.kronos.pulsBbus.core.MessageQueueManager;
import com.kronos.pulsBbus.core.batch.BatchConsumeConfig;
import com.kronos.pulsBbus.core.batch.BatchConsumeResult;
import com.kronos.pulsBbus.core.batch.BatchSupportMessageTemplate;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class PulseBusCoreApplicationTests {

    @Test
    void contextLoads() {
    }


    @Resource
    private BatchSupportMessageTemplate batchTemplate;

    /**
     * 订阅批量消息处理
     */
    public void subscribeBatchMessages() {
        // 配置批量消费参数
        BatchConsumeConfig config = new BatchConsumeConfig();
        config.setBatchSize(50);           // 每批50条消息
        config.setBatchTimeoutMs(2000);    // 2秒超时
        config.setMaxWaitTimeMs(10000);    // 最大等待10秒
        config.setEnablePartialConsume(true);
        config.setMaxRetryCount(3);

        // 订阅批量消息
        batchTemplate.subscribeBatch("order.batch.topic", messages -> {
            System.out.println("开始批量处理订单消息，数量: " + messages.size());

            BatchConsumeResult result = new BatchConsumeResult(messages.size());

            // 批量处理业务逻辑
            for (Message message : messages) {
                try {
                    // 处理单个订单
                    processOrder(message.getPayload());
                    result.addResult(ConsumeResult.SUCCESS, message.getId());
                } catch (Exception e) {
                    System.err.println("处理订单失败: " + message.getId() + ", 错误: " + e.getMessage());
                    result.addResult(ConsumeResult.RETRY, message.getId());
                }
            }

            System.out.println("批量处理完成 - 成功: " + result.getSuccessCount() +
                    ", 失败: " + result.getFailedCount());

            return result;
        }, config);
    }

    /**
     * 处理单个订单
     */
    private void processOrder(Object orderData) {
        // 模拟订单处理逻辑
        System.out.println("处理订单: " + orderData);

        // 模拟处理时间
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Resource
    private MessageQueueManager messageQueueManager;

    @Test
    void testTemplate() throws InterruptedException {
        MessageQueueTemplate template = messageQueueManager.getTemplate();
        new Thread(() -> {
            for (int i = 0; i < 50000; i++) {
                template.send("order.topic", "订单数据" + i);
            }

        }).start();

        new Thread(() -> {
            template.subscribe("order.topic", message -> {
                System.out.println("收到消息: " + message.getPayload());
                return ConsumeResult.SUCCESS;
            });
        }).start();

        Thread.sleep(5000);
    }
}
