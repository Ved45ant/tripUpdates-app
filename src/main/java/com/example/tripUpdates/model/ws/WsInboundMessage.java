package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an inbound WebSocket message from a client (Employee, Admin, or Security).
 * Used for SUBSCRIBE/UNSUBSCRIBE/RESUME actions.
 */
public class WsInboundMessage {

    @JsonProperty("action")
    private String action; // "SUBSCRIBE", "UNSUBSCRIBE", "RESUME"

    @JsonProperty("topic")
    private String topic; // "trip:{id}" or "office:{id}"

    @JsonProperty("lastSeq")
    private long lastSeq; // 0 if new connection, > 0 if resuming

    public WsInboundMessage() {}

    public WsInboundMessage(String action, String topic, long lastSeq) {
        this.action = action;
        this.topic = topic;
        this.lastSeq = lastSeq;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public void setLastSeq(long lastSeq) {
        this.lastSeq = lastSeq;
    }
}
