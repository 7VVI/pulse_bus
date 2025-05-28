package com.kronos.pulsBbus.core;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:27
 * @desc 消费结果
 */
public enum ConsumeResult {
    SUCCESS,        // 消费成功
    RETRY,          // 需要重试
    FAILED,         // 消费失败，不再重试
    SUSPEND        // 暂停消费
}
