package com.kronos.pulsBbus.disruptor;

import com.lmax.disruptor.*;

/**
 * @author zhangyh
 * @Date 2025/6/3 8:42
 * @desc
 */
public class DisruptorUtils {

    /**
     * 创建等待策略
     */
    public static WaitStrategy createWaitStrategy(String type) {
        switch (type) {
            case "BLOCKING":
                return new BlockingWaitStrategy();
            case "BUSY_SPIN":
                return new BusySpinWaitStrategy();
            case "YIELDING":
                return new YieldingWaitStrategy();
            case "SLEEPING":
                return new SleepingWaitStrategy();
            case "TIMEOUT_BLOCKING":
                return new TimeoutBlockingWaitStrategy(5, java.util.concurrent.TimeUnit.SECONDS);
            default:
                return new BlockingWaitStrategy();
        }
    }

    /**
     * 创建生产者类型
     */
    public static com.lmax.disruptor.dsl.ProducerType createProducerType(String type) {
        switch (type) {
            case "SINGLE":
                return com.lmax.disruptor.dsl.ProducerType.SINGLE;
            case "MULTI":
                return com.lmax.disruptor.dsl.ProducerType.MULTI;
            default:
                return com.lmax.disruptor.dsl.ProducerType.MULTI;
        }
    }

    /**
     * 验证RingBuffer大小是否为2的幂
     */
    public static boolean isPowerOfTwo(int size) {
        return size > 0 && (size & (size - 1)) == 0;
    }

    /**
     * 获取下一个2的幂
     */
    public static int nextPowerOfTwo(int size) {
        if (isPowerOfTwo(size)) {
            return size;
        }

        int power = 1;
        while (power < size) {
            power <<= 1;
        }
        return power;
    }
}
