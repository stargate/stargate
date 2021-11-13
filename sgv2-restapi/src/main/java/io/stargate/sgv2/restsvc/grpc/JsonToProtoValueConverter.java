package io.stargate.sgv2.restsvc.grpc;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import java.math.BigInteger;

public class JsonToProtoValueConverter {
  public static QueryOuterClass.Value NULL =
      QueryOuterClass.Value.newBuilder()
          .setNull(QueryOuterClass.Value.Null.newBuilder().build())
          .build();

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
    }
    if (javaValue instanceof Boolean) {
      return Values.of((Boolean) javaValue);
    }
    throw new IllegalArgumentException("Unsupported Java type: " + javaValue.getClass().getName());
  }
}
