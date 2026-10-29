package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * SNAPSHOT frame sent during reconnection when a client's lastSeq has been evicted from the Redis replay buffer.
 * Contains the latest absolute state (location, status) to allow full recovery without partial replay.
 * Follows up with live EVENT frames from the new sequence onwards.
 */
public class WsSnapshotFrame extends WsOutboundFrame {

    @JsonProperty("message")
    private String message; // Human-readable explanation

    @JsonProperty("data")
    private JsonNode data; // Latest state snapshot (latestLocation, latestStatus)

    public WsSnapshotFrame() {
        super();
    }

    public WsSnapshotFrame(String topic, String message, JsonNode data) {
        super("SNAPSHOT", topic);
        this.message = message;
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }
}
