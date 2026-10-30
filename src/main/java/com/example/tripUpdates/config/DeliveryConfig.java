package com.example.tripUpdates.config;

import com.example.tripUpdates.service.core.ConnectionRegistry;
import com.example.tripUpdates.service.core.ConnectionWriter;
import com.example.tripUpdates.service.core.DeliveryObserver;
import com.example.tripUpdates.service.core.EventFanOut;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;

/** Wires the framework-agnostic delivery engine (service.core) into Spring. */
@Configuration
public class DeliveryConfig {

    @Bean
    public ConnectionRegistry connectionRegistry() {
        return new ConnectionRegistry();
    }

    /** One virtual thread per drain task: a blocked client only blocks its own task. */
    @Bean(destroyMethod = "close")
    public ConnectionWriter connectionWriter(DeliveryObserver observer,
                                             @Value("${delivery.send-timeout-ms:5000}") long sendTimeoutMillis) {
        return new ConnectionWriter(Executors.newVirtualThreadPerTaskExecutor(), observer, sendTimeoutMillis);
    }

    /** Totals computed on scrape (O(connections)); never one series per connection. */
    @Bean
    public MeterBinder outboxGauges(ConnectionRegistry registry) {
        return meters -> {
            Gauge.builder("outbox.pending.events", registry, r -> {
                long[] sum = {0};
                r.forEachSession(s -> sum[0] += s.getQueueSize());
                return sum[0];
            }).description("Events pending across all connection outboxes").register(meters);
            Gauge.builder("outbox.pending.max", registry, r -> {
                long[] max = {0};
                r.forEachSession(s -> max[0] = Math.max(max[0], s.getQueueSize()));
                return max[0];
            }).description("Deepest single connection outbox").register(meters);
        };
    }

    @Bean
    public EventFanOut eventFanOut(ConnectionRegistry registry, ConnectionWriter writer, DeliveryObserver observer) {
        return new EventFanOut(registry, writer, observer);
    }
}
