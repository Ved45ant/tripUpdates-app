package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base class for all outbound WebSocket frames sent from the server to clients.
 * Uses JSON polymorphism to distinguish frame types: ACK, ERROR, EVENT, SNAPSHOT.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = WsAckFrame.class, name = "ACK"),
        @JsonSubTypes.Type(value = WsErrorFrame.class, name = "ERROR"),
        @JsonSubTypes.Type(value = WsEventFrame.class, name = "EVENT"),
        @JsonSubTypes.Type(value = WsSnapshotFrame.class, name = "SNAPSHOT")
})
public abstract class WsOutboundFrame {

    @JsonProperty("type")
    private String type;

    @JsonProperty("topic")
    private String topic;

    public WsOutboundFrame() {}

    public WsOutboundFrame(String type, String topic) {
        this.type = type;
        this.topic = topic;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
