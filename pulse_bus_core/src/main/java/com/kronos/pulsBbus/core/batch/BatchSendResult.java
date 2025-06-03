package com.kronos.pulsBbus.core.batch;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:26
 * @desc
 */

import com.kronos.pulsBbus.core.single.SendResult;

import java.io.Serializable;

/**
 * 批量发送结果
 */
public class BatchSendResult implements Serializable {
    private int                        totalCount;
    private int                        successCount;
    private int                        failedCount;
    private java.util.List<SendResult> results;

    public BatchSendResult(int totalCount) {
        this.totalCount = totalCount;
        this.results = new java.util.ArrayList<>();
    }

    // getter/setter方法
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

    public java.util.List<SendResult> getResults() {
        return results;
    }

    public void addResult(SendResult result) {
        results.add(result);
    }
}
