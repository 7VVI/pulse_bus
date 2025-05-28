package com.kronos.pulsBbus.disruptor.batch;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:27
 * @desc
 */
public class BatchConsumeEventFactory implements com.lmax.disruptor.EventFactory<BatchConsumeEvent> {
    @Override
    public BatchConsumeEvent newInstance() {
        return new BatchConsumeEvent();
    }
}
