package com.example.tripUpdates.repository.redis;

import com.example.tripUpdates.service.core.EnvelopeJson;
import com.example.tripUpdates.service.core.EventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read side of the replay buffer (written by {@code publish.lua}).
 *
 * The buffer holds only STATUS and SOS events (bounded, newest N per topic). Locations are never
 * buffered: a resuming client gets just the latest location per cab from the snapshot, which is
 * what keeps location spam from evicting the events that must not be lost.
 */
@Repository
public class RedisReplayBufferRepository {

    /**
     * Atomic view for a resume.
     *
     * @param currentSeq    latest sequence assigned for the topic (0 if none)
     * @param criticalFloor highest STATUS/SOS sequence already evicted (0 if none)
     * @param events        buffered STATUS/SOS with seq &gt; lastSeq, ascending
     * @param snapshot      latest state: field ("loc:{cab}", "status:{trip}", "sos:{trip}") to envelope
     */
    public record ResumeData(long currentSeq, long criticalFloor, List<EventEnvelope> events,
                             Map<String, EventEnvelope> snapshot) {
    }

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<List> resumeScript;

    public RedisReplayBufferRepository(StringRedisTemplate redis) {
        this.redis = redis;
        this.resumeScript = new DefaultRedisScript<>();
        this.resumeScript.setLocation(new ClassPathResource("scripts/resume.lua"));
        this.resumeScript.setResultType(List.class);
    }

    public ResumeData read(String topic, long lastSeq) {
        List<?> r = redis.execute(resumeScript,
                List.of(RedisKeys.seq(topic), RedisKeys.criticalBuffer(topic), RedisKeys.criticalFloor(topic),
                        RedisKeys.snapshot(topic)),
                String.valueOf(Math.max(lastSeq, 0)));
        try {
            long currentSeq = ((Number) r.get(0)).longValue();
            long floor = ((Number) r.get(1)).longValue();
            List<EventEnvelope> events = new ArrayList<>();
            for (Object json : (List<?>) r.get(2)) {
                events.add(EnvelopeJson.parse(json.toString()));
            }
            List<?> flat = (List<?>) r.get(3);
            Map<String, EventEnvelope> snapshot = new LinkedHashMap<>();
            for (int i = 0; i + 1 < flat.size(); i += 2) {
                snapshot.put(flat.get(i).toString(), EnvelopeJson.parse(flat.get(i + 1).toString()));
            }
            return new ResumeData(currentSeq, floor, events, snapshot);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt envelope in Redis for topic " + topic, e);
        }
    }

    public long currentSeq(String topic) {
        String v = redis.opsForValue().get(RedisKeys.seq(topic));
        return v == null ? 0L : Long.parseLong(v);
    }

    public long criticalFloor(String topic) {
        String v = redis.opsForValue().get(RedisKeys.criticalFloor(topic));
        return v == null ? 0L : Long.parseLong(v);
    }

    public long criticalBufferSize(String topic) {
        Long n = redis.opsForZSet().zCard(RedisKeys.criticalBuffer(topic));
        return n == null ? 0L : n;
    }
}
