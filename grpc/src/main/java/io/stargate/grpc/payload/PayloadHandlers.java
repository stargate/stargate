package io.stargate.grpc.payload;

import io.stargate.grpc.payload.cql.ValuesHandler;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import java.util.EnumMap;

public class PayloadHandlers {
  public static final EnumMap<Payload.Type, PayloadHandler> HANDLERS =
      new EnumMap<>(Payload.Type.class);

  static {
    HANDLERS.put(Type.TYPE_CQL, new ValuesHandler());
  }
}
