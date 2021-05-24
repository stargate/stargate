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
package io.stargate.grpc.payload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.stargate.grpc.payload.cql.ValuesHandler;
import io.stargate.proto.QueryOuterClass.Payload;

public class PayloadHandlers {

  private static final ImmutableMap<Payload.Type, PayloadHandler> HANDLERS =
      Maps.immutableEnumMap(
          ImmutableMap.<Payload.Type, PayloadHandler>builder()
              .put(Payload.Type.CQL, new ValuesHandler())
              .build());

  @NonNull
  public static PayloadHandler get(Payload.Type type) {
    PayloadHandler handler = HANDLERS.get(type);
    if (handler == null) {
      throw Status.UNIMPLEMENTED.withDescription("Unsupported payload type").asRuntimeException();
    }
    return handler;
  }
}
