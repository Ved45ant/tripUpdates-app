package com.example.tripUpdates.model.ws;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ERROR frame sent in response to a failed SUBSCRIBE action.
 * Contains a generic error message that does not leak internal state or tenant information.
 * Prevents enumeration attacks by not confirming the existence of requested topics.
 */
public class WsErrorFrame extends WsOutboundFrame {

    @JsonProperty("code")
    private int code; // HTTP status code (e.g., 403, 400)

    @JsonProperty("message")
    private String message; // Generic error message

    public WsErrorFrame() {
        super();
    }

    public WsErrorFrame(String topic, int code, String message) {
        super("ERROR", topic);
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
