package org.apache.cassandra.transport;

import java.util.UUID;

public class MessageHandler {
  public static UUID getTracingId(Message.Response response) {
    return response.getTracingId();
  }
}
