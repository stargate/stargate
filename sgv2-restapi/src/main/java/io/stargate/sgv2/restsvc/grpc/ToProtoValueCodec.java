package io.stargate.sgv2.restsvc.grpc;

import io.stargate.bridge.proto.QueryOuterClass;

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

  /**
   * Accessor for "key" codec, if type being handled has one: non-null for Map types, {@code null}
   * for all other types.
   *
   * @return Value codec this codec uses, if any; {@code null} if none
   */
  public abstract ToProtoValueCodec getKeyCodec();

  /**
   * Accessor for "value" codec, if type being handled has one: non-null for Container types (like
   * {@code set}, {@code map}, {@code list}), {@code null} for simple types.
   *
   * @return Value codec this codec uses, if any; {@code null} if none
   */
  public abstract ToProtoValueCodec getValueCodec();
}
