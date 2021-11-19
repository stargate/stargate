package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

public abstract class ToProtoValueCodec {
  public abstract QueryOuterClass.Value protoValueFromStrictlyTyped(Object value);

  public abstract QueryOuterClass.Value protoValueFromStringified(String value);

  public QueryOuterClass.Value protoValueFromLooselyTyped(Object value) {
    if (value instanceof String) {
      return protoValueFromStringified((String) value);
    }
    return protoValueFromStrictlyTyped(value);
  }
}
