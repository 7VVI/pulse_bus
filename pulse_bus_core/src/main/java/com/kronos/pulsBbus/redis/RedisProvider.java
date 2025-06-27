package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.MessageQueueProvider;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:46
 * @desc
 */
@org.springframework.stereotype.Component
public class RedisProvider implements MessageQueueProvider {

    private org.springframework.data.redis.core.RedisTemplate<String, Object> redisTemplate;

    @Override
    public String getProviderName() {
        return "Redis";
    }

    @Override
    public MessageQueueTemplate getTemplate() {
        return new RedisTemplateAdapter(redisTemplate);
    }

    @Override
    public <T extends BaseProviderConfig> void initialize(T config) {

    }


    @Override
    public boolean isHealthy() {
        try {
            return redisTemplate.getConnectionFactory() != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void destroy() {
        // 清理Redis连接
    }
}
