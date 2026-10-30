package com.example.tripUpdates.exception;

import com.example.tripUpdates.telemetry.PiiSanitizer;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

/**
 * One place that turns exceptions into HTTP responses.
 *
 * <ul>
 *   <li><b>Clients</b> get a fixed message per status ("Bad Request", "Forbidden", ...) and, for 500 only, an
 *       {@code errorId}. Nothing from the exception, the request or the stack is ever echoed back.</li>
 *   <li><b>Operators</b> get one log line per error with the {@code errorId}, the exception chain (class names and
 *       sanitized messages, see {@link PiiSanitizer}) and a short stack trace of the top frames. The exception object
 *       itself is never passed to the logger, because its message may contain personal data (a malformed
 *       JSON body with an email address, a database error quoting a row).</li>
 * </ul>
 * Spring MVC's own exceptions (unreadable body, wrong method, unsupported media type, ...) go through the same path.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
                                                             HttpStatusCode statusCode, WebRequest request) {
        HttpStatus status = HttpStatus.resolve(statusCode.value());
        if (status != null && status.is5xxServerError()) {
            return respond(ex, status, null, null, logger::error);
        }
        return respond(ex, status != null ? status : HttpStatus.BAD_REQUEST, null, null, logger::warn);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Object> handleIllegalArgument(IllegalArgumentException ex) {
        return respond(ex, HttpStatus.BAD_REQUEST, null, null, logger::warn);
    }

    @ExceptionHandler({UnauthorizedSubscriptionException.class, AccessDeniedException.class})
    public ResponseEntity<Object> handleForbidden(RuntimeException ex) {
        return respond(ex, HttpStatus.FORBIDDEN, null, null, logger::warn);
    }

    @ExceptionHandler(InvalidTripTransitionException.class)
    public ResponseEntity<Object> handleConflict(InvalidTripTransitionException ex) {
        return respond(ex, HttpStatus.CONFLICT, null, null, logger::warn);
    }

    @ExceptionHandler(PublishUnavailableException.class)
    public ResponseEntity<Object> handleUnavailable(PublishUnavailableException ex) {
        return respond(ex, HttpStatus.SERVICE_UNAVAILABLE, null, "1", logger::error);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleGeneric(Exception ex) {
        return respond(ex, HttpStatus.INTERNAL_SERVER_ERROR, UUID.randomUUID().toString(), null, logger::error);
    }

    private ResponseEntity<Object> respond(Exception ex, HttpStatus status, String errorId, String retryAfter,
                                           java.util.function.Consumer<String> log) {
        String id = errorId != null ? errorId : (status.is5xxServerError() ? UUID.randomUUID().toString() : null);
        log.accept("request failed status=" + status.value() + (id != null ? " errorId=" + id : "")
                + " cause=[" + PiiSanitizer.describe(ex, 5) + "] at=" + topFrames(ex));
        ErrorResponse body = new ErrorResponse(status.value(), status.getReasonPhrase(), Instant.now().toString(),
                status.is5xxServerError() ? id : null);
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(status);
        if (retryAfter != null) {
            builder.header("Retry-After", retryAfter);
        }
        return builder.body(body);
    }

    /** Code locations only (class.method:line); no messages, so nothing personal can be in here. */
    private static String topFrames(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return Arrays.stream(root.getStackTrace()).limit(6)
                .map(f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName() + ":" + f.getLineNumber())
                .reduce((a, b) -> a + " < " + b).orElse("-");
    }

    /** The only body clients ever see. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ErrorResponse {
        private final int status;
        private final String message;
        private final String timestamp;
        private final String errorId;

        public ErrorResponse(int status, String message, String timestamp, String errorId) {
            this.status = status;
            this.message = message;
            this.timestamp = timestamp;
            this.errorId = errorId;
        }

        public int getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getErrorId() {
            return errorId;
        }
    }
}
