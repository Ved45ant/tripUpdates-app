package com.example.tripUpdates.telemetry;

import java.util.regex.Pattern;

/**
 * Removes personal data and credentials from free text before it is logged or reported:
 * emails, phone numbers (international, grouped, 10-digit mobile), IP addresses, URLs, JWTs/bearer tokens
 * and {@code password=...}-style secrets.
 *
 * Identifiers the operators need (UUIDs, topic names, sequence numbers, epoch timestamps) are deliberately
 * left intact. This is the second line of defence: the code does not log personal data in the first place.
 * The identical list of patterns is configured in {@code logback.xml} as a value mask on every log line.
 */
public final class PiiSanitizer {

    private static final Object[][] RULES = {
            new Object[]{Pattern.compile("\\b[A-Za-z][A-Za-z0-9+.-]*://\\S+"), "[REDACTED_URL]"},
            new Object[]{Pattern.compile("eyJ[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]{5,}\\.[A-Za-z0-9_-]*"), "[REDACTED_TOKEN]"},
            new Object[]{Pattern.compile("(?i)bearer\\s+[A-Za-z0-9._~+/=-]+"), "[REDACTED_TOKEN]"},
            new Object[]{Pattern.compile("(?i)\\b(?:password|passwd|pwd|secret|apikey|api-key|token|authorization)\\b\\s*[:=]\\s*\\S+"), "[REDACTED_CREDENTIALS]"},
            new Object[]{Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"), "[REDACTED_EMAIL]"},
            new Object[]{Pattern.compile("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b"), "[REDACTED_IP]"},
            new Object[]{Pattern.compile("\\+\\d[\\d\\s().-]{7,}\\d"), "[REDACTED_PHONE]"},
            new Object[]{Pattern.compile("(?<![\\w.-])\\(?\\d{3}\\)?[\\s.-]\\d{3}[\\s.-]\\d{4}(?![\\w-])"), "[REDACTED_PHONE]"},
            new Object[]{Pattern.compile("(?<![\\w.-])\\d{5}[\\s-]\\d{5}(?![\\w-])"), "[REDACTED_PHONE]"},
            new Object[]{Pattern.compile("(?<![\\w.-])[6-9]\\d{9}(?![\\w.-])"), "[REDACTED_PHONE]"}
    };

    private PiiSanitizer() {
    }

    public static String sanitize(String text) {
        if (text == null) {
            return null;
        }
        String out = text;
        for (Object[] rule : RULES) {
            out = ((Pattern) rule[0]).matcher(out).replaceAll((String) rule[1]);
        }
        return out;
    }

    /** One line per exception in the cause chain (class + sanitized message), at most {@code maxDepth} deep. */
    public static String describe(Throwable t, int maxDepth) {
        StringBuilder sb = new StringBuilder();
        Throwable current = t;
        for (int depth = 0; current != null && depth < maxDepth; depth++) {
            if (depth > 0) {
                sb.append(" <- ");
            }
            sb.append(current.getClass().getSimpleName());
            String message = sanitize(current.getMessage());
            if (message != null && !message.isBlank()) {
                sb.append(": ").append(message.length() > 300 ? message.substring(0, 300) + "..." : message);
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return sb.toString();
    }
}
