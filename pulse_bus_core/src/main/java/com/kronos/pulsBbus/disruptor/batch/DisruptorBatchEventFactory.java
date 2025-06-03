package com.kronos.pulsBbus.disruptor.batch;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:45
 * @desc
 */

import com.lmax.disruptor.EventFactory;

/**
 * Disruptor批量事件工厂
 */
public class DisruptorBatchEventFactory implements EventFactory<DisruptorBatchEvent> {
    @Override
    public DisruptorBatchEvent newInstance() {
        return new DisruptorBatchEvent();
    }
}
