package com.kronos.pulsBbus.core.single;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:26
 * @desc
 */

import java.io.Serializable;

/**
 * 消息发送结果
 */
public class SendResult implements Serializable {
    private boolean success;
    private String messageId;
    private String errorMessage;
    private long timestamp;

    // 构造函数和getter/setter
    public SendResult(boolean success, String messageId) {
        this.success = success;
        this.messageId = messageId;
        this.timestamp = System.currentTimeMillis();
    }

    public SendResult(boolean success, String messageId, String errorMessage) {
        this.success = success;
        this.messageId = messageId;
        this.errorMessage = errorMessage;
    }

    public boolean isSuccess() { return success; }
    public String getMessageId() { return messageId; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public long getTimestamp() { return timestamp; }
}