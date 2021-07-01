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

import static io.stargate.grpc.codec.cql.ValueCodec.PROTOCOL_VERSION;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.stargate.db.Result;
import io.stargate.proto.QueryOuterClass.TraceEvent;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceEventsMapper {
  private static final Logger log = LoggerFactory.getLogger(TraceEventsMapper.class);

  public static List<TraceEvent> toTraceEvents(Result.Rows rows) {

    List<TraceEvent> traceEvents = new ArrayList<>();
    for (List<ByteBuffer> row : rows.rows) {
      traceEvents.add(
          // we rely on the ordering of data in the row.
          // It is determined by the columns ordering in the system_traces.events query
          TraceEvent.newBuilder()
              .setActivity(TypeCodecs.TEXT.decode(row.get(0), PROTOCOL_VERSION))
              .setSource(inetAddressToString(row.get(1)))
              .setSourceElapsed(toInt(row))
              .setThread(TypeCodecs.TEXT.decode(row.get(3), PROTOCOL_VERSION))
              .build());
    }
    return traceEvents;
  }

  private static Integer toInt(List<ByteBuffer> row) {
    return TypeCodecs.INT.decode(row.get(2), PROTOCOL_VERSION);
  }

  private static String inetAddressToString(ByteBuffer value) {
    byte[] bytes = value.array();
    if (bytes.length == 0) {
      return "";
    }

    try {
      return InetAddress.getByAddress(bytes).toString();
    } catch (Exception ex) {
      log.warn("Problem when getting tracing source value.");
      return "";
    }
  }
}
