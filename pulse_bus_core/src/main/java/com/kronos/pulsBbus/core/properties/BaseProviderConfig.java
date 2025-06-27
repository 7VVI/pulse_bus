package com.kronos.pulsBbus.core.properties;

/**
 * @author zhangyh
 * @Date 2025/6/27 17:32
 * @desc
 */
public interface BaseProviderConfig {

    /**
     * 消息队列提供者名称
     */
    String getName();

    /**
     * 是否启用
     * @return true/false
     */
    Boolean isEnable();
}
