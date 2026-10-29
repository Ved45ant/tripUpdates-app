package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ACK frame sent in response to a successful SUBSCRIBE action.
 * Indicates the client has been successfully subscribed to the topic.
 */
public class WsAckFrame extends WsOutboundFrame {

    @JsonProperty("status")
    private String status; // "SUBSCRIBED"

    /**
     * Latest sequence of the topic when the subscription became active. A client that subscribed with
     * {@code lastSeq == 0} uses it as its resume baseline: everything before it is history it was never owed
     * (it gets a SNAPSHOT instead), everything critical after it arrives live or via replay.
     */
    @JsonProperty("seq")
    private long seq;

    public WsAckFrame() {
        super();
    }

    public WsAckFrame(String topic, String status) {
        super("ACK", topic);
        this.status = status;
    }

    public WsAckFrame(String topic, String status, long seq) {
        this(topic, status);
        this.seq = seq;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
