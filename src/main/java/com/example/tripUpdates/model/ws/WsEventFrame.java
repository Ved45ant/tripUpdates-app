package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * EVENT frame pushing live updates to a subscribed client.
 * Contains event type, sequence number, and typed data payload.
 * Sequence number allows clients to deduplicate and detect gaps.
 */
public class WsEventFrame extends WsOutboundFrame {

    @JsonProperty("seq")
    private long seq; // Sequence number for ordering and deduplication

    @JsonProperty("eventType")
    private String eventType; // "LOCATION", "STATUS", "SOS"

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("data")
    private JsonNode data; // Flexible payload structure

    public WsEventFrame() {
        super();
    }

    public WsEventFrame(String topic, long seq, String eventType, long timestamp, JsonNode data) {
        super("EVENT", topic);
        this.seq = seq;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.data = data;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }
}
