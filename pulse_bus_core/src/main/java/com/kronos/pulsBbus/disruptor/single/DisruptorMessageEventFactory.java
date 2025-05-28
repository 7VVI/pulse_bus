package com.kronos.pulsBbus.disruptor.single;

/**
 * @author zhangyh
 * @Date 2025/5/28 16:39
 * @desc
 */
public class DisruptorMessageEventFactory implements com.lmax.disruptor.EventFactory<DisruptorMessageEvent> {
    @Override
    public DisruptorMessageEvent newInstance() {
        return new DisruptorMessageEvent();
    }
}