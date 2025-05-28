package com.kronos.pulsBbus.core;

import com.kronos.pulsBbus.core.single.MessageQueueTemplate;

import java.util.Map;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:27
 * @desc 消息队列提供者插件接口
 */
public interface MessageQueueProvider {

    /**
     * 获取提供者名称
     */
    String getProviderName();

    /**
     * 获取消息队列模板实例
     */
    MessageQueueTemplate getTemplate();

    /**
     * 初始化提供者
     */
    void initialize(Map<String, Object> config);

    /**
     * 健康检查
     */
    boolean isHealthy();

    /**
     * 销毁资源
     */
    void destroy();
}
