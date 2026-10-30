package com.example.tripUpdates.config;

import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.EventFanOut;
import com.example.tripUpdates.service.pubsub.PubSubGateway;
import com.example.tripUpdates.telemetry.MetricsService;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the Redis pub/sub gateway (own Lettuce connections, independent of Spring's listener container). */
@Configuration
public class PubSubConfig {

    @Bean(destroyMethod = "shutdown")
    public RedisClient pubSubRedisClient(@Value("${spring.data.redis.host:localhost}") String host,
                                         @Value("${spring.data.redis.port:6379}") int port,
                                         @Value("${spring.data.redis.password:}") String password) {
        return PubSubGateway.newClient(host, port, password);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public PubSubGateway pubSubGateway(RedisClient pubSubRedisClient,
                                       ConnectionRegistry registry,
                                       EventFanOut fanOut,
                                       RedisReplayBufferRepository replayBuffer,
                                       MeterRegistry meters,
                                       MetricsService metricsService,
                                       @Value("${pubsub.dispatch-stripes:4}") int stripes,
                                       @Value("${pubsub.dispatch-queue:50000}") int queue,
                                       @Value("${pubsub.gap-grace-ms:200}") long gapGraceMillis) {
        PubSubGateway gateway = new PubSubGateway(pubSubRedisClient, registry, fanOut, replayBuffer,
                new PubSubGateway.Config(stripes, queue, gapGraceMillis));
        gateway.setMetrics(new PubSubGateway.Metrics() {
            @Override
            public void lagMillis(long millis) {
                metricsService.recordPubSubLagMillis(millis);
            }

            @Override
            public void redisError(String op) {
                metricsService.recordRedisError(op);
            }
        });
        FunctionCounter.builder("pubsub.messages.received", gateway, g -> g.received()).register(meters);
        FunctionCounter.builder("pubsub.messages.dropped", gateway, g -> g.dropped()).register(meters);
        FunctionCounter.builder("pubsub.messages.parse_errors", gateway, g -> g.parseErrors()).register(meters);
        FunctionCounter.builder("pubsub.gap.repairs", gateway, g -> g.gapRepairs()).register(meters);
        FunctionCounter.builder("pubsub.resyncs", gateway, g -> g.resyncs()).register(meters);
        FunctionCounter.builder("pubsub.reconnects", gateway, g -> g.reconnects()).register(meters);
        return gateway;
    }
}
