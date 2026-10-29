package com.example.tripUpdates.service.core;

import com.example.tripUpdates.model.ws.WsSnapshotFrame;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository;
import com.example.tripUpdates.repository.redis.RedisReplayBufferRepository.ResumeData;
import com.example.tripUpdates.service.core.ReplayDecision.Action;
import com.example.tripUpdates.telemetry.MetricsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Resume after reconnect. Reads (currentSeq, critical floor, buffered STATUS/SOS, snapshot) in one
 * atomic Redis step and applies {@link ReplayDecision}:
 *
 * <ul>
 *   <li>NOTHING  - client is current</li>
 *   <li>REPLAY   - missed STATUS/SOS in order, plus only the latest location per cab</li>
 *   <li>SNAPSHOT - one SNAPSHOT frame with the latest state of the topic</li>
 * </ul>
 *
 * The session must already be subscribed and in buffering state
 * ({@link ClientConnectionSession#startBuffering}): live events published while the replay is read
 * are parked and delivered after it, and the per-topic dedup window removes any overlap.
 */
@Service
public class RecoveryService {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    private final RedisReplayBufferRepository replayBuffer;
    private final EventFanOut fanOut;
    private final ObjectMapper objectMapper;
    private final MetricsService metrics;

    public RecoveryService(RedisReplayBufferRepository replayBuffer, EventFanOut fanOut, ObjectMapper objectMapper) {
        this(replayBuffer, fanOut, objectMapper, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public RecoveryService(RedisReplayBufferRepository replayBuffer, EventFanOut fanOut, ObjectMapper objectMapper,
                           MetricsService metrics) {
        this.replayBuffer = replayBuffer;
        this.fanOut = fanOut;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    /** Latest sequence assigned on the topic (0 if none): the baseline reported in the SUBSCRIBED ACK. */
    public long currentSeq(String topic) {
        return replayBuffer.currentSeq(topic);
    }

    /** @return the action taken, or null if the session/arguments were invalid */
    public Action handleResume(ClientConnectionSession session, String topic, long lastSeq) {
        if (session == null || topic == null || lastSeq < 0) {
            return null;
        }
        ResumeData data = replayBuffer.read(topic, lastSeq);
        Action action = ReplayDecision.decide(lastSeq, data.currentSeq(), data.criticalFloor());

        switch (action) {
            case NOTHING:
                break;
            case REPLAY:
                for (EventEnvelope event : data.events()) {
                    fanOut.deliverReplay(session, event);
                }
                for (Map.Entry<String, EventEnvelope> latest : data.snapshot().entrySet()) {
                    if (latest.getKey().startsWith("loc:") && latest.getValue().seq() > lastSeq) {
                        fanOut.deliverReplay(session, latest.getValue());
                    }
                }
                break;
            case SNAPSHOT:
                sendSnapshot(session, topic, data);
                break;
        }
        if (metrics != null) {
            metrics.count("resume.total", "action", action.name());
        }
        logger.info("Resume handled: session={}, topic={}, lastSeq={}, action={}, replayed={}",
                session.getSessionId(), topic, lastSeq, action,
                action == Action.REPLAY ? data.events().size() : 0);
        return action;
    }

    private void sendSnapshot(ClientConnectionSession session, String topic, ResumeData data) {
        ObjectNode latest = objectMapper.createObjectNode();
        for (Map.Entry<String, EventEnvelope> e : data.snapshot().entrySet()) {
            ObjectNode item = objectMapper.createObjectNode();
            item.put("seq", e.getValue().seq());
            item.put("eventType", e.getValue().type().name());
            item.put("timestamp", e.getValue().ingestTs());
            try {
                item.set("data", objectMapper.readTree(e.getValue().payload()));
            } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
                continue;
            }
            latest.set(e.getKey(), item);
        }
        ObjectNode body = objectMapper.createObjectNode();
        body.put("currentSeq", data.currentSeq());
        body.set("latest", latest);
        session.sendMessage(new WsSnapshotFrame(topic, "Resuming from latest state.", body));
    }
}
