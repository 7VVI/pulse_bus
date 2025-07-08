package com.kronos.pulsBbus.redis;

import com.kronos.pulsBbus.core.MessageQueueProvider;
import com.kronos.pulsBbus.core.monitor.MessageQueueMetrics;
import com.kronos.pulsBbus.core.properties.BaseProviderConfig;
import com.kronos.pulsBbus.core.single.MessageQueueTemplate;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author zhangyh
 * @Date 2025/5/28 15:46
 * @desc Redis消息队列提供者
 */
@org.springframework.stereotype.Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "message.queue.provider",
        havingValue = "redis"
)
public class RedisProvider implements MessageQueueProvider {

    private static final Logger log = LoggerFactory.getLogger(RedisProvider.class);

    @Resource
    private MessageQueueMetrics metrics;

    private RedisTemplate<String, Object> redisTemplate;
    private RedisTemplateAdapter template;
    private RedisProperties properties;
    private volatile boolean initialized = false;

    @Override
    public String getProviderName() {
        return "redis";
    }

    @Override
    public MessageQueueTemplate getTemplate() {
        if (template == null) {
            throw new IllegalStateException("Redis provider not initialized");
        }
        return template;
    }

    @Override
    public <T extends BaseProviderConfig> void initialize(T config) {
        if (initialized) {
            return;
        }
        
        this.properties = (RedisProperties) config;
        
        try {
            // 创建Redis连接工厂
            LettuceConnectionFactory connectionFactory = createConnectionFactory();
            connectionFactory.afterPropertiesSet();
            
            // 创建RedisTemplate
            this.redisTemplate = createRedisTemplate(connectionFactory);
            
            // 创建模板适配器
            this.template = new RedisTemplateAdapter(redisTemplate, properties, metrics);
            
            this.initialized = true;
            log.info("Redis消息队列提供者初始化成功");
            
        } catch (Exception e) {
            log.error("Redis消息队列提供者初始化失败", e);
            throw new RuntimeException("Failed to initialize Redis provider", e);
        }
    }

    private LettuceConnectionFactory createConnectionFactory() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                properties.getHost(), 
                properties.getPort()
        );
        
        if (properties.getPassword() != null) {
            factory.setPassword(properties.getPassword());
        }
        
        factory.setDatabase(properties.getDatabase());
        factory.setTimeout(properties.getTimeout());
        
        return factory;
    }

    private RedisTemplate<String, Object> createRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        // 设置序列化器
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        GenericJackson2JsonRedisSerializer jsonSerializer = new GenericJackson2JsonRedisSerializer();
        
        template.setKeySerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);
        
        template.afterPropertiesSet();
        return template;
    }

    @Override
    public boolean isHealthy() {
        try {
            if (redisTemplate == null) {
                return false;
            }
            
            // 执行ping命令检查连接
            String result = redisTemplate.getConnectionFactory()
                    .getConnection()
                    .ping();
            return "PONG".equals(result);
            
        } catch (Exception e) {
            log.warn("Redis健康检查失败", e);
            return false;
        }
    }

    @Override
    public void destroy() {
        try {
            if (template != null) {
                template.destroy();
            }
            
            if (redisTemplate != null && redisTemplate.getConnectionFactory() != null) {
                redisTemplate.getConnectionFactory().getConnection().close();
            }
            
            initialized = false;
            log.info("Redis消息队列提供者已销毁");
            
        } catch (Exception e) {
            log.error("销毁Redis提供者时发生错误", e);
        }
    }
}
