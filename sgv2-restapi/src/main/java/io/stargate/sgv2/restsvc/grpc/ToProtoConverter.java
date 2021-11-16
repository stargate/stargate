package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;

/**
 * Converter that knows how to convert a set of column values (either as a set, or one by one)
 * expressed as "Java values" ({@code java.lang.Object} nominally} into "Bridge" Stargate Protobuf
 * result values. This conversion is usually based on Cassandra Schema metadata due to strict typing
 * used by Bridge gRPC service.
 */
public class ToProtoConverter {
  /**
   * Method that will convert a "partially typed" Java value (conforming to "natural" binding from
   * JSON) into Bridge Protobuf {@link QueryOuterClass.Value}.
   */
  public QueryOuterClass.Value protoValueFromJsonTyped(Object value) {
    // !!! To implement
    return null;
  }

  /**
   * Method that will convert an "untyped" value (simple Java String) into Bridge Protobuf {@link
   * QueryOuterClass.Value}.
   *
   * <p>Note that despite seeming simplicity, there are non-trivial rules on encoding of structured
   * values.
   */
  public QueryOuterClass.Value protoValueFromStringified(String value) {
    // !!! To implement
    return null;
  }
}
