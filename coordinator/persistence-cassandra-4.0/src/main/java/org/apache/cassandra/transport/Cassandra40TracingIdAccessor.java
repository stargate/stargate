package org.apache.cassandra.transport;

import java.util.UUID;

/**
 * Helper class needed to access tracing id from Message.Response to copy it to converted response.
 */
public class Cassandra40TracingIdAccessor {
  // Needed because C-4.0 does not expose `getTracingId()` as public unlike 3.11
  public static UUID getTracingId(Message.Response response) {
    return response.getTracingId();
  }
}
