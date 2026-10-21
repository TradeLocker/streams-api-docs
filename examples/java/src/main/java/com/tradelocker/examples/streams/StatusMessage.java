package com.tradelocker.examples.streams;

import java.util.Arrays;
import org.json.JSONObject;

/**
 * Payload of the {@code connection} event and of every {@code subscriptions} acknowledgement
 * (asyncapi/streams-api.asyncapi.yaml is the contract). Branch on {@code code}, not on
 * {@code message}: the wording of messages can change.
 *
 * @param remainingRequests SUBSCRIBE requests left in the rate-limit window; {@code null} when omitted
 */
public record StatusMessage(String status, String code, String message, Integer remainingRequests) {

  /** What this example reports when no acknowledgement arrived in time, or the connection closed first. */
  static final StatusMessage ACK_TIMEOUT = new StatusMessage("error", "ackTimeout", "no acknowledgement", null);

  boolean ok() {
    return "ok".equals(status);
  }

  /** Reads the first argument of an event or acknowledgement; io.socket passes JSON objects as {@link JSONObject}. */
  static StatusMessage from(Object... args) {
    if (args.length > 0 && args[0] instanceof JSONObject json) {
      var remaining = json.has("remainingRequests") ? Integer.valueOf(json.optInt("remainingRequests")) : null;
      return new StatusMessage(json.optString("status"), json.optString("code"), json.optString("message"), remaining);
    }
    // Anything else is unexpected: treat it like a server error, which is retried with backoff.
    return new StatusMessage("error", "unknown", "unexpected payload " + Arrays.toString(args), null);
  }
}
