package com.kronos.pulsBbus.core;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:26
 * @desc
 */
/**
 * 事务回调接口
 */
public interface TransactionCallback {
    boolean doInTransaction();
    void rollback();
}
