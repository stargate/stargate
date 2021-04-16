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
