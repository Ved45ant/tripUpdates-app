package com.example.tripUpdates.telemetry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class PiiSanitizerTest {

    /** Personal data and secrets that must never survive, with the fragment that identifies them. */
    public static final String[][] PII = {
            {"mail alice.smith+tag@example.co.uk now", "alice"},
            {"phone +91 98765 43210 end", "98765"},
            {"phone 415-555-2671 end", "555-2671"},
            {"phone (415) 555 2671 end", "555 2671"},
            {"phone 415.555.2671 end", "555.2671"},
            {"phone 98765 43210 end", "98765"},
            {"phone 9876543210 end", "9876543210"},
            {"ip 10.0.0.5 end", "10.0.0.5"},
            {"see https://internal.example.com/users/42?x=1 end", "internal.example.com"},
            {"jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r_wW1gFWFOEjXk end", "eyJhbGci"},
            {"header Bearer abc123.def456-ghi end", "abc123"},
            {"config password=hunter2 end", "hunter2"},
            {"config token: s3cr3t-value end", "s3cr3t"},
    };

    /** Identifiers operators need to follow an event. These must be left alone. */
    public static final String[] KEEP = {
            "trip 123e4567-e89b-12d3-a456-426614174000 seq=42",
            "topic=trip:123e4567-e89b-12d3-a456-426614174000 ingestTs=1715421000000",
            "session 8f14e45f-ceea-467f-a0e6-1b3f5a1c9b4d reason=send-timeout",
            "count=1234567890123 latency=250ms version 3.2.0",
    };

    @Test
    void removesPersonalDataAndSecrets() {
        for (String[] sample : PII) {
            String out = PiiSanitizer.sanitize(sample[0]);
            assertThat(out).as(sample[0]).doesNotContain(sample[1]).contains("[REDACTED_");
        }
    }

    @Test
    void leavesOperationalIdentifiersAlone() {
        for (String text : KEEP) {
            assertThat(PiiSanitizer.sanitize(text)).as(text).isEqualTo(text);
        }
    }

    @Test
    void nullAndEmptyAreSafe() {
        assertThat(PiiSanitizer.sanitize(null)).isNull();
        assertThat(PiiSanitizer.sanitize("")).isEmpty();
    }

    @Test
    void describeShowsTheCauseChainWithoutPersonalData() {
        Exception e = new RuntimeException("outer for bob@corp.com", new IllegalStateException("row 415-555-2671 locked"));
        String text = PiiSanitizer.describe(e, 5);
        assertThat(text).contains("RuntimeException").contains("IllegalStateException").contains("<-")
                .doesNotContain("bob").doesNotContain("555-2671");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void describeBoundsTheChainDepth(int depth) {
        Throwable t = new RuntimeException("e0", new RuntimeException("e1", new RuntimeException("e2", new RuntimeException("e3"))));
        assertThat(PiiSanitizer.describe(t, depth).split("<-")).hasSize(depth);
    }
}
