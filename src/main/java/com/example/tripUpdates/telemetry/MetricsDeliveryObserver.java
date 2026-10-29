package com.example.tripUpdates.telemetry;

import com.example.tripUpdates.service.core.DeliveryObserver;
import org.springframework.stereotype.Component;

/** Bridges the core engine's {@link DeliveryObserver} hooks to Micrometer. */
@Component
public class MetricsDeliveryObserver implements DeliveryObserver {

    private final MetricsService metrics;

    public MetricsDeliveryObserver(MetricsService metrics) {
        this.metrics = metrics;
    }

    @Override
    public void sosDelivered(long latencyMillis) {
        metrics.recordSosLatencyMillis(latencyMillis);
    }

    @Override
    public void conflated() {
        metrics.incrementConflatedCount();
    }

    @Override
    public void outboxDepth(int depth) {
        metrics.recordOutboxDepth(depth);
    }

    @Override
    public void disconnected(String reason) {
        metrics.recordDisconnect(reason);
    }
}
