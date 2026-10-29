package com.example.tripUpdates.service.core;

/**
 * Builds EVENT frames as JSON text without a serialisation round trip: the payload is already
 * JSON, so per-subscriber cost is a single string build.
 */
final class EventFrames {

    private EventFrames() {
    }

    static String event(EventEnvelope e) {
        String payload = e.payload() == null ? "null" : e.payload();
        StringBuilder sb = new StringBuilder(96 + payload.length());
        sb.append("{\"type\":\"EVENT\",\"topic\":\"");
        escapeInto(sb, e.topic());
        sb.append("\",\"seq\":").append(e.seq())
                .append(",\"eventType\":\"").append(e.type().name())
                .append("\",\"timestamp\":").append(e.ingestTs())
                .append(",\"data\":").append(payload)
                .append('}');
        return sb.toString();
    }

    private static void escapeInto(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
    }
}
