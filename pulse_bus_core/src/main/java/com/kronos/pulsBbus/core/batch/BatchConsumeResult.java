package com.kronos.pulsBbus.core.batch;

import com.kronos.pulsBbus.core.ConsumeResult;

import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:55
 * @desc
 */
public class BatchConsumeResult {
    private int                 totalCount;
    private int                 successCount;
    private int                 failedCount;
    private List<ConsumeResult> results;
    private List<String>        failedMessageIds;

    public BatchConsumeResult(int totalCount) {
        this.totalCount = totalCount;
        this.results = new java.util.ArrayList<>();
        this.failedMessageIds = new java.util.ArrayList<>();
    }

    // Getters and Setters
    public int getTotalCount() {
        return totalCount;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
    }

    public List<ConsumeResult> getResults() {
        return results;
    }

    public List<String> getFailedMessageIds() {
        return failedMessageIds;
    }

    /**
     * 添加消费结果
     */
    public void addResult(ConsumeResult result, String messageId) {
        results.add(result);
        if (result == ConsumeResult.SUCCESS) {
            successCount++;
        } else {
            failedCount++;
            failedMessageIds.add(messageId);
        }
    }
}