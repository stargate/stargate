package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

/**
 * Abstraction for codecs that read incoming "REST" values (either structured JSON or, for path
 * parameters at least, "Stringified" alternative) and then decode them as Bridge/gRPC values. This
 * is needed for binding values for CQL operations sent to Bridge.
 */
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
