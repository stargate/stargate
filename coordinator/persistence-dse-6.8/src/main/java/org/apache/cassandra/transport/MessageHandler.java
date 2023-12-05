package org.apache.cassandra.transport;

import java.util.UUID;

/**
 * Helper class needed to access tracing id from Message.Response to copy it to converted response.
 */
public class MessageHandler {
  public static UUID getTracingId(Message.Response response) {
    return response.getTracingId();
  }
}
