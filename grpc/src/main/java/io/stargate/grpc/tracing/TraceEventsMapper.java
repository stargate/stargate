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
import io.stargate.proto.QueryOuterClass.TraceEvent;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class TraceEventsMapper {
  public static List<TraceEvent> toTraceEvents(List<Row> rows) {

    List<TraceEvent> traceEvents = new ArrayList<>();
    for (Row row : rows) {
      traceEvents.add(
          TraceEvent.newBuilder()
              .setActivity(row.getString("activity"))
              .setSource(inetAddressToString(row.getInetAddress("source")))
              .setSourceElapsed(row.getInt("source_elapsed"))
              .setThread(row.getString("thread"))
              .build());
    }
    return traceEvents;
  }

  private static String inetAddressToString(InetAddress value) {
    if (value == null) {
      return "";
    } else return value.toString();
  }
}
