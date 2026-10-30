package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.entity.EventArchive;
import com.example.tripUpdates.repository.EventArchiveRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch insert; a unique (trip, sequence) constraint makes retries idempotent. If a batch hits a
 * duplicate, it falls back to row-by-row inserts and skips only the duplicates.
 */
@Component
public class JpaArchiveSink implements ArchiveWriter.Sink {

    private final EventArchiveRepository repository;

    public JpaArchiveSink(EventArchiveRepository repository) {
        this.repository = repository;
    }

    @Override
    public void saveBatch(List<ArchiveWriter.Record> batch) {
        List<EventArchive> rows = new ArrayList<>(batch.size());
        for (ArchiveWriter.Record r : batch) {
            rows.add(new EventArchive(r.tripId(), r.type(), r.seq(), r.payload()));
        }
        try {
            repository.saveAll(rows);
        } catch (DataIntegrityViolationException duplicateInBatch) {
            for (EventArchive row : rows) {
                try {
                    repository.save(row);
                } catch (DataIntegrityViolationException alreadyArchived) {
                    // idempotent: this (trip, seq) is already stored
                }
            }
        }
    }
}
