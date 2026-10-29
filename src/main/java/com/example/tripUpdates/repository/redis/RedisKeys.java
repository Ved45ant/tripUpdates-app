package com.example.tripUpdates.repository.redis;

/** Redis key and channel naming. A "topic" is e.g. {@code trip:{id}}, {@code cab:{id}}, {@code office:{id}}. */
public final class RedisKeys {

    public static final String EVENT_CHANNEL_PREFIX = "evt:";
    public static final String SOS_CHANNEL_PREFIX = "sos:";

    private RedisKeys() {
    }

    public static String seq(String topic) {
        return "seq:" + topic;
    }

    public static String criticalBuffer(String topic) {
        return "buf:" + topic + ":crit";
    }

    public static String criticalFloor(String topic) {
        return "floor:" + topic;
    }

    public static String snapshot(String topic) {
        return "snap:" + topic;
    }

    public static String lastLocation(String cabId) {
        return "lastloc:" + cabId;
    }

    public static String dedup(String topic, String eventId) {
        return "dedup:" + topic + ":" + eventId;
    }

    public static String eventChannel(String topic) {
        return EVENT_CHANNEL_PREFIX + topic;
    }

    public static String sosChannel(String topic) {
        return SOS_CHANNEL_PREFIX + topic;
    }
}
