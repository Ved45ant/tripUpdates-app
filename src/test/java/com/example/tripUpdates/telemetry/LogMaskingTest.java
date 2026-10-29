package com.example.tripUpdates.telemetry;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Loads the real production logback.xml and checks what actually comes out on stdout. */
class LogMaskingTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final PrintStream originalOut = System.out;
    private ByteArrayOutputStream captured;
    private LoggerContext context;

    @BeforeEach
    void setUp() throws Exception {
        captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        context = new LoggerContext();
        context.setMDCAdapter(new ch.qos.logback.classic.util.LogbackMDCAdapter());
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(getClass().getResource("/logback.xml"));
        List<String> problems = new ArrayList<>();
        context.getStatusManager().getCopyOfStatusList().stream()
                .filter(st -> st.getLevel() >= ch.qos.logback.core.status.Status.WARN)
                .forEach(st -> problems.add(st.getMessage()));
        assertThat(problems).as("logback.xml configuration problems").isEmpty();
    }

    @AfterEach
    void tearDown() {
        context.stop();
        System.setOut(originalOut);
    }

    private List<JsonNode> lines() throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String line : captured.toString(StandardCharsets.UTF_8).split("\\R")) {
            if (!line.isBlank()) {
                out.add(mapper.readTree(line));
            }
        }
        return out;
    }

    @Test
    void everyLineIsStructuredJsonWithTheOperationalFields() throws Exception {
        Logger log = context.getLogger("com.example.tripUpdates.test");
        context.getMDCAdapter().put("sessionId", "s-42");
        log.info("subscribed topic={} seq={}", "trip:123e4567-e89b-12d3-a456-426614174000", 42);

        JsonNode line = lines().get(0);
        assertThat(line.get("level").asText()).isEqualTo("INFO");
        assertThat(line.get("logger_name").asText()).isEqualTo("com.example.tripUpdates.test");
        assertThat(line.get("@timestamp").asText()).isNotBlank();
        assertThat(line.get("app").asText()).isEqualTo("tripupdates");
        assertThat(line.get("node").asText()).isNotBlank();
        assertThat(line.get("sessionId").asText()).isEqualTo("s-42");
        assertThat(line.get("message").asText()).isEqualTo("subscribed topic=trip:123e4567-e89b-12d3-a456-426614174000 seq=42");
    }

    @Test
    void personalDataIsMaskedEvenWhenCodeLogsItByMistake() throws Exception {
        Logger log = context.getLogger("com.example.tripUpdates.test");
        for (String[] sample : PiiSanitizerTest.PII) {
            log.warn(sample[0]);
        }
        log.error("lookup failed", new IllegalStateException("no row for dave@corp.com phone +91 98765 43210"));

        String all = captured.toString(StandardCharsets.UTF_8);
        for (String[] sample : PiiSanitizerTest.PII) {
            assertThat(all).as("leaked: " + sample[1]).doesNotContain(sample[1]);
        }
        assertThat(all).doesNotContain("dave@corp.com").doesNotContain("98765");
        JsonNode error = lines().get(lines().size() - 1);
        assertThat(error.get("stack_trace").asText()).contains("IllegalStateException").contains("[REDACTED_EMAIL]");
    }

    @Test
    void theLogMaskAndTheCodeSanitizerAgree() throws Exception {
        Logger log = context.getLogger("com.example.tripUpdates.test");
        List<String> inputs = new ArrayList<>();
        for (String[] sample : PiiSanitizerTest.PII) {
            inputs.add(sample[0]);
        }
        for (String keep : PiiSanitizerTest.KEEP) {
            inputs.add(keep);
        }
        inputs.forEach(log::info);

        List<JsonNode> out = lines();
        for (int i = 0; i < inputs.size(); i++) {
            assertThat(out.get(i).get("message").asText()).as(inputs.get(i)).isEqualTo(PiiSanitizer.sanitize(inputs.get(i)));
        }
    }
}
