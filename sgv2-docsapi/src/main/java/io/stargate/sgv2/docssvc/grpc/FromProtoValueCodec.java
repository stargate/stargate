package io.stargate.sgv2.docssvc.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.proto.QueryOuterClass;

/**
 * Interface for low-level handles that convert a single column value defined in the "external"
 * Stargate Protobuf result into Java value that can be serialized by frontend framework (like
 * DropWizard)
 */
public abstract class FromProtoValueCodec {
  public abstract Object fromProtoValue(QueryOuterClass.Value value);

  public abstract JsonNode jsonNodeFrom(QueryOuterClass.Value value);
}
