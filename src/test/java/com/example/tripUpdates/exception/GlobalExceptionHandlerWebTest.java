package com.example.tripUpdates.exception;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/** What a client sees, and what lands in the log, when things go wrong with personal data in the error text. */
@WebMvcTest(controllers = GlobalExceptionHandlerWebTest.Boom.class)
@AutoConfigureMockMvc(addFilters = false)
@org.springframework.context.annotation.Import({com.example.tripUpdates.security.JwtTokenProvider.class, GlobalExceptionHandlerWebTest.Boom.class})
class GlobalExceptionHandlerWebTest {

    @RestController
    public static class Boom {
        public static class Body {
            public String name;
        }

        @GetMapping("/t/bad")
        public String bad() {
            throw new IllegalArgumentException("bad input from alice@example.com on 415-555-2671");
        }

        @GetMapping("/t/boom")
        public String boom() {
            throw new IllegalStateException("db password=hunter2 at jdbc://10.0.0.5 for bob@corp.com",
                    new RuntimeException("row of +91 98765 43210"));
        }

        @GetMapping("/t/denied")
        public String denied() {
            throw new AccessDeniedException("user carol@example.org may not");
        }

        @GetMapping("/t/conflict")
        public String conflict() {
            throw new InvalidTripTransitionException("trip of dave@example.org cannot go back");
        }

        @GetMapping("/t/down")
        public String down() {
            throw new PublishUnavailableException("redis at 10.1.2.3 refused", new RuntimeException("erin@example.org"));
        }

        @PostMapping("/t/json")
        public String json(@RequestBody Body body) {
            return body.name;
        }
    }

    static final List<String> PII_FRAGMENTS = List.of("alice", "bob", "carol", "dave", "erin", "415-555", "hunter2",
            "10.0.0.5", "10.1.2.3", "98765", "jdbc", "example.com", "corp.com", "example.org");

    @Autowired
    MockMvc mvc;

    private final ObjectMapper mapper = new ObjectMapper();
    private ListAppender<ILoggingEvent> appender;
    private Logger handlerLogger;

    @BeforeEach
    void attach() {
        handlerLogger = (Logger) LoggerFactory.getLogger(GlobalExceptionHandler.class);
        appender = new ListAppender<>();
        appender.start();
        handlerLogger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        handlerLogger.detachAppender(appender);
    }

    private JsonNode body(org.springframework.test.web.servlet.ResultActions r, int status) throws Exception {
        String text = r.andReturn().getResponse().getContentAsString();
        assertThat(r.andReturn().getResponse().getStatus()).isEqualTo(status);
        for (String fragment : PII_FRAGMENTS) {
            assertThat(text).as("response leaked " + fragment).doesNotContain(fragment);
        }
        assertThat(text).doesNotContain("Exception").doesNotContain("at com.").doesNotContain("stackTrace");
        return mapper.readTree(text);
    }

    @Test
    void clientsGetFixedMessagesAndNothingFromTheException() throws Exception {
        JsonNode bad = body(mvc.perform(get("/t/bad")), 400);
        assertThat(bad.get("message").asText()).isEqualTo("Bad Request");
        assertThat(bad.has("errorId")).isFalse();

        assertThat(body(mvc.perform(get("/t/denied")), 403).get("message").asText()).isEqualTo("Forbidden");
        assertThat(body(mvc.perform(get("/t/conflict")), 409).get("message").asText()).isEqualTo("Conflict");

        var down = mvc.perform(get("/t/down"));
        JsonNode downBody = body(down, 503);
        assertThat(down.andReturn().getResponse().getHeader("Retry-After")).isEqualTo("1");
        assertThat(downBody.get("errorId").asText()).isNotBlank();
    }

    @Test
    void springsOwnRequestErrorsAreGenericToo() throws Exception {
        // malformed JSON containing an email address: the parser message quotes the input
        JsonNode malformed = body(mvc.perform(post("/t/json").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\": \"alice@example.com\" \"oops\"")), 400);
        assertThat(malformed.get("message").asText()).isEqualTo("Bad Request");
        assertThat(body(mvc.perform(post("/t/bad")), 405).get("message").asText()).isEqualTo("Method Not Allowed");
        assertThat(body(mvc.perform(post("/t/json").contentType(MediaType.TEXT_PLAIN).content("alice@example.com")), 415)
                .get("message").asText()).isEqualTo("Unsupported Media Type");
    }

    @Test
    void anUnexpectedErrorReturnsAnErrorIdThatFindsTheSanitizedLogLine() throws Exception {
        JsonNode response = body(mvc.perform(get("/t/boom")), 500);
        String errorId = response.get("errorId").asText();
        assertThat(response.get("message").asText()).isEqualTo("Internal Server Error");

        List<String> logged = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(logged).anySatisfy(line -> assertThat(line)
                .contains("errorId=" + errorId).contains("IllegalStateException").contains("RuntimeException"));
    }

    @Test
    void logsNeverContainPersonalDataOrSecretsFromTheExceptionText() throws Exception {
        for (String path : List.of("/t/bad", "/t/boom", "/t/denied", "/t/conflict", "/t/down")) {
            mvc.perform(get(path));
        }
        mvc.perform(post("/t/json").contentType(MediaType.APPLICATION_JSON).content("{\"name\": \"alice@example.com\" \"oops\""));

        assertThat(appender.list).isNotEmpty();
        for (ILoggingEvent event : appender.list) {
            assertThat(event.getThrowableProxy()).as("the exception object is not handed to the logger").isNull();
            for (String fragment : PII_FRAGMENTS) {
                assertThat(event.getFormattedMessage()).as("log leaked " + fragment).doesNotContain(fragment);
            }
        }
    }
}
