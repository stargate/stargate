package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

public abstract class ToProtoValueCodec {
  public abstract QueryOuterClass.Value protoValueFromJsonTyped(Object value);

  public abstract QueryOuterClass.Value protoValueFromStringified(String value);
}
