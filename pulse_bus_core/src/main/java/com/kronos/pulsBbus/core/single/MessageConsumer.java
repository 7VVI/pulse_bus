package com.kronos.pulsBbus.core.single;

import com.kronos.pulsBbus.core.ConsumeResult;
import com.kronos.pulsBbus.core.Message;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:27
 * @desc 消息消费者接口
 */
@FunctionalInterface
public interface MessageConsumer {
    /**
     * 消费消息
     * @param message 消息对象
     * @return 消费结果
     */
    ConsumeResult consume(Message message);
}
