package com.example.tripUpdates.service.business;

import com.example.tripUpdates.model.enums.EventType;

/** Publishes one event to one topic and returns what happened. Implemented on Redis; faked in tests. */
public interface TopicPublisher {

    /**
     * @param seq       sequence assigned on the topic (for a duplicate: the original one; 0 if stale)
     * @param duplicate the eventId was already published to this topic; nothing was published again
     * @param stale     a LOCATION older than the latest one already accepted for the cab; dropped
     */
    record PublishResult(long seq, boolean duplicate, boolean stale) {
    }

    /**
     * @param eventTimestamp cab-supplied event time, only used for the stale-location guard
     * @param guardStale     when true and the type is LOCATION, an event older than the latest accepted
     *                       one for the cab is dropped
     * @throws org.springframework.dao.DataAccessException if the backing store is unavailable
     */
    PublishResult publish(String topic, EventType type, String eventId, String cabId, String tripId,
                          long ingestTs, long eventTimestamp, String payloadJson, boolean guardStale);
}
