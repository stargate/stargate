package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

/**
 * Interface for low-level handles that convert a single column value defined in the "external"
 * Stargate Protobuf result into Java value that can be serialized by frontend framework (like
 * DropWizard)
 */
public abstract class ExtProtoValueCodec {
  public abstract Object fromProtoValue(QueryOuterClass.Value value);
}
