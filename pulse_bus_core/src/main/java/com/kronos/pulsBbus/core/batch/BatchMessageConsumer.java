package com.kronos.pulsBbus.core.batch;

import com.kronos.pulsBbus.core.Message;

import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:55
 * @desc 批量消息消费者接口
 */
@FunctionalInterface
public interface BatchMessageConsumer {
    /**
     * 批量消费消息
     *
     * @param messages 消息列表
     * @return 批量消费结果
     */
    BatchConsumeResult consumeBatch(List<Message> messages);
}
