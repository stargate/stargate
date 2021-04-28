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

import io.stargate.grpc.payload.cql.ValuesHandler;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class PayloadHandlers {
  public static final Map<Type, PayloadHandler> HANDLERS =
      Collections.unmodifiableMap(
          new EnumMap<Type, PayloadHandler>(Payload.Type.class) {
            {
              put(Type.TYPE_CQL, new ValuesHandler());
            }
          });
}
