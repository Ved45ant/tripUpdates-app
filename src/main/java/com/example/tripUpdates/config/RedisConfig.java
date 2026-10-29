package com.example.tripUpdates.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis configuration for the TripUpdates system.
 * Configures RedisTemplate beans for different data types:
 * - String operations (sequence counters via INCR)
 * - Generic object operations (events, snapshots)
 * - Sorted set operations (replay buffers)
 * - Hash operations (latest snapshots)
 *
 * All RedisTemplate instances are configured with Jackson serialization
 * for proper polymorphic event and object handling.
 */
@Configuration
public class RedisConfig {

    /**
     * RedisTemplate for generic Object operations.
     * Used for storing and retrieving serialized event objects.
     * Supports polymorphic event types (LOCATION, STATUS, SOS).
     * Uses Jackson2JsonRedisSerializer for JSON serialization.
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory,
                                                        ObjectMapper objectMapper) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // String key serializer
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);

        // Jackson2JsonRedisSerializer for value serialization
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer =
                new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);

        template.setValueSerializer(jackson2JsonRedisSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(jackson2JsonRedisSerializer);

        template.afterPropertiesSet();
        return template;
    }

    /**
     * ObjectMapper bean for JSON serialization.
     * Configured to handle polymorphic types through Jackson annotations.
     * Enables @JsonTypeInfo and @JsonSubTypes support for event dispatch.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    /**
     * RedisMessageListenerContainer for Pub/Sub message handling.
     * Used by RedisPubSubListener to subscribe to Redis channels.
     * Configures the thread pool for message processing.
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }
}
