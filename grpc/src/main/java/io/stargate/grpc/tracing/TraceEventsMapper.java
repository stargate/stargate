/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.tracing;

import io.stargate.db.datastore.Row;
import io.stargate.db.tracing.TracingData;
import io.stargate.proto.QueryOuterClass.Traces;
import io.stargate.proto.QueryOuterClass.Traces.Event;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TraceEventsMapper {
  public static Traces toTraceEvents(TracingData tracingData, String tracingId) {

    List<Event> traceEvents = new ArrayList<>();
    for (Row row : tracingData.getEvents()) {
      UUID eventId = row.getUuid("event_id");

      traceEvents.add(
          Event.newBuilder()
              .setActivity(row.getString("activity"))
              .setSource(inetAddressToString(row.getInetAddress("source")))
              .setSourceElapsed(row.getInt("source_elapsed"))
              .setThread(row.getString("thread"))
              .setEventId(eventId == null ? "" : eventId.toString())
              .build());
    }

    Instant startedAt = tracingData.getSessionRow().getInstant("started_at");

    return Traces.newBuilder()
        .setDuration(tracingData.getSessionRow().getInt("duration"))
        .setId(tracingId)
        .addAllEvents(traceEvents)
        .setStartedAt(startedAt == null ? -1 : startedAt.toEpochMilli())
        .build();
  }

  private static String inetAddressToString(InetAddress value) {
    if (value == null) {
      return "";
    } else return value.toString();
  }
}
