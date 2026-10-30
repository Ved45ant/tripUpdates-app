package com.example.tripUpdates.config;

import com.example.tripUpdates.service.business.ArchiveWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IngestConfig {

    /** Queue of 10k critical events, batches of up to 100 every 200 ms, 5 attempts per batch. */
    @Bean(destroyMethod = "close")
    public ArchiveWriter archiveWriter(ArchiveWriter.Sink sink,
                                       @Value("${archive.queue-capacity:10000}") int capacity,
                                       @Value("${archive.batch-size:100}") int batchSize,
                                       @Value("${archive.flush-ms:200}") long flushMillis) {
        return new ArchiveWriter(sink, capacity, batchSize, flushMillis, 5, 200);
    }
}
