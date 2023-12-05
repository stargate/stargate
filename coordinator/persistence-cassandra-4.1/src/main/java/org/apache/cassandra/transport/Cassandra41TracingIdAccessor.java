package org.apache.cassandra.transport;

import java.util.UUID;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Helper class needed to access tracing id from Message.Response to copy it to converted response.
 */
public class Cassandra41TracingIdAccessor {
  // Needed because C-4.1 does not expose `getTracingId()` as public unlike 3.11
  public static UUID getTracingId(Message.Response response) {
    TimeUUID tracingId = response.getTracingId();
    return (tracingId == null) ? null : tracingId.asUUID();
  }
}
