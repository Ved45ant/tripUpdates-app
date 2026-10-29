package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.enums.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Wire format of an {@link EventEnvelope} inside Redis (replay buffer, snapshot, pub/sub):
 * <pre>{"topic":"trip:1","type":"STATUS","cabId":"c1","ingestTs":1715421000000,"payload":{...},"seq":42}</pre>
 * The Lua publish script appends {@code ,"seq":N}} to a {@link #prefix prefix}, because the
 * sequence number is assigned inside Redis.
 */
public final class EnvelopeJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EnvelopeJson() {
    }

    /** Envelope JSON without its closing brace and without seq (the script completes it). */
    public static String prefix(String topic, EventType type, String cabId, long ingestTs, String payloadJson)
            throws JsonProcessingException {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("topic", topic);
        node.put("type", type.name());
        if (cabId != null) {
            node.put("cabId", cabId);
        }
        node.put("ingestTs", ingestTs);
        node.set("payload", payloadJson == null ? MAPPER.nullNode() : MAPPER.readTree(payloadJson));
        String json = node.toString();
        return json.substring(0, json.length() - 1);
    }

    public static EventEnvelope parse(String json) throws JsonProcessingException {
        JsonNode node = MAPPER.readTree(json);
        JsonNode cab = node.get("cabId");
        JsonNode payload = node.get("payload");
        return new EventEnvelope(
                node.get("topic").asText(),
                node.get("seq").asLong(),
                EventType.valueOf(node.get("type").asText()),
                cab == null || cab.isNull() ? null : cab.asText(),
                node.get("ingestTs").asLong(),
                payload == null ? "null" : payload.toString());
    }

    /** Re-serialises an envelope (used where a Redis-shaped message has to be rebuilt). */
    public static String toJson(EventEnvelope e) throws JsonProcessingException {
        String prefix = prefix(e.topic(), e.type(), e.cabId(), e.ingestTs(), e.payload());
        return prefix + ",\"seq\":" + e.seq() + "}";
    }
}
