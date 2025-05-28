package com.kronos.pulsBbus.disruptor;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:42
 * @desc
 */
public  class DisruptorConfig {
    private int bufferSize = 1024;
    private String waitStrategy = "BlockingWaitStrategy";
    private int consumerThreads = 1;
    private boolean enableMetrics = true;

    // Getters and Setters
    public int getBufferSize() { return bufferSize; }
    public void setBufferSize(int bufferSize) { this.bufferSize = bufferSize; }
    public String getWaitStrategy() { return waitStrategy; }
    public void setWaitStrategy(String waitStrategy) { this.waitStrategy = waitStrategy; }
    public int getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }
    public boolean isEnableMetrics() { return enableMetrics; }
    public void setEnableMetrics(boolean enableMetrics) { this.enableMetrics = enableMetrics; }
}

