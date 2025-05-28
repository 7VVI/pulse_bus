package com.kronos.pulsBbus.core.batch;

import com.kronos.pulsBbus.core.single.MessageQueueTemplate;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:57
 * @desc 扩展MessageQueueTemplate接口支持批量消费
 */
public interface BatchMessageQueueTemplate extends MessageQueueTemplate {

    /**
     * 批量订阅消息
     */
    void subscribeBatch(String topic, BatchMessageConsumer consumer, BatchConsumeConfig config);

    /**
     * 取消批量订阅
     */
    void unsubscribeBatch(String topic);

    /**
     * 获取批量消费统计信息
     */
    Map<String, Object> getBatchConsumeStats(String topic);
}
