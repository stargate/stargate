package io.stargate.sgv2.restsvc.grpc;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Helper class for converting "natural" Java object types (such as ones bound by Jackson when
 * target is {@code java.lang.Object}) into matching gRPC {@link QueryOuterClass.Value} types.
 *
 * <p>NOTE: current implementation is incomplete and may not prove useful without additional schema
 * information.
 */
public class JsonToProtoValueConverter {
  private JsonToProtoValueConverter() {}

  public static QueryOuterClass.Value protoValueFor(Object javaValue) {
    if (javaValue == null) {
      return Values.NULL;
    }
    if (javaValue instanceof String) {
      return Values.of((String) javaValue);
    }
    if (javaValue instanceof Number) {
      if (javaValue instanceof Long) {
        return Values.of((Long) javaValue);
      }
      if (javaValue instanceof Integer) {
        return Values.of((Integer) javaValue);
      }
      if (javaValue instanceof BigInteger) {
        return Values.of((BigInteger) javaValue);
      }
      if (javaValue instanceof Double) {
        return Values.of((Double) javaValue);
      }
      if (javaValue instanceof Float) {
        return Values.of((Float) javaValue);
      }
      if (javaValue instanceof BigDecimal) {
        return Values.of((BigDecimal) javaValue);
      }
    }
    if (javaValue instanceof Boolean) {
      return Values.of((Boolean) javaValue);
    }
    throw new IllegalArgumentException("Unsupported Java type: " + javaValue.getClass().getName());
  }
}
