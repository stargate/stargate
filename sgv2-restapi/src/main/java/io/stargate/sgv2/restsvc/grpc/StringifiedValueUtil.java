package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.Collection;

/** Helper class that deals with "Stringified" variants of structured values. */
public class StringifiedValueUtil {
  public static void decodeStringifiedCollection(
      String value, ToProtoValueCodec elementCodec, Collection<QueryOuterClass.Value> results) {
    return;
  }

  public static void decodeStringifiedMap(
      String value,
      ToProtoValueCodec keyCodec,
      ToProtoValueCodec valueCodec,
      Collection<QueryOuterClass.Value> results) {
    return;
  }
}
