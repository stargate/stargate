package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.Map;

/**
 * Converter that knows how to convert a set of column values (either as a set, or one by one)
 * expressed as "Java values" ({@code java.lang.Object} nominally} into "Bridge" Stargate Protobuf
 * result values. This conversion is usually based on Cassandra Schema metadata due to strict typing
 * used by Bridge gRPC service.
 */
public class ToProtoConverter {
  protected final String tableName;
  protected final Map<String, ToProtoValueCodec> codecsByName;

  protected ToProtoConverter(final String tableName, Map<String, ToProtoValueCodec> codecsByName) {
    this.tableName = tableName;
    this.codecsByName = codecsByName;
  }

  /**
   * Method that will convert a "partially typed" Java value (conforming to "natural" binding from
   * JSON) into Bridge Protobuf {@link QueryOuterClass.Value} optionally considering "stringified"
   * alternatives for values (that is: any CQL type may be passed as JSON String, and further
   * decoded and coerced as necessary).
   */
  public QueryOuterClass.Value protoValueFromLooselyTyped(String fieldName, Object value) {
    return protoValueFromLooselyTyped(getCodec(fieldName), fieldName, value);
  }

  public QueryOuterClass.Value protoValueFromLooselyTyped(
      ToProtoValueCodec codec, String fieldName, Object value) {
    try {
      if (value instanceof String) {
        // Need to allow optional use of "extra" single quotes
        String strValue = StringifiedValueUtil.handleSingleQuotes((String) value);
        return codec.protoValueFromStringified(strValue);
      }
      return codec.protoValueFromLooselyTyped(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to deserialize field '%s', problem: %s",
              fullFieldName(fieldName), e.getMessage()),
          e);
    }
  }

  /**
   * Method that will convert a "partially typed" Java value (conforming to "natural" binding from
   * JSON) into Bridge Protobuf {@link QueryOuterClass.Value} but WITHOUT considering "stringified"
   * alternatives.
   */
  public QueryOuterClass.Value protoValueFromStrictlyTyped(String fieldName, Object value) {
    final ToProtoValueCodec codec = getCodec(fieldName);
    try {
      return codec.protoValueFromStrictlyTyped(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to deserialize field '%s', problem: %s",
              fullFieldName(fieldName), e.getMessage()),
          e);
    }
  }

  /**
   * Method that will convert an "untyped" value (simple Java String) into Bridge Protobuf {@link
   * QueryOuterClass.Value}.
   *
   * <p>Note that despite seeming simplicity, there are non-trivial rules on encoding of structured
   * values.
   */
  public QueryOuterClass.Value protoValueFromStringified(String fieldName, String value) {
    return protoValueFromStringified(getCodec(fieldName), fieldName, value);
  }

  public QueryOuterClass.Value protoValueFromStringified(
      ToProtoValueCodec codec, String fieldName, String value) {
    try {
      // Need to allow optional use of "extra" single quotes
      String strValue = StringifiedValueUtil.handleSingleQuotes(value);
      return codec.protoValueFromStringified(strValue);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to deserialize field '%s', problem: %s",
              fullFieldName(fieldName), e.getMessage()),
          e);
    }
  }

  public ToProtoValueCodec getCodec(String fieldName) {
    ToProtoValueCodec codec = codecsByName.get(fieldName);
    if (codec == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown field name '%s' (table '%s'); known fields: %s",
              fieldName, tableName, codecsByName.keySet()));
    }
    return codec;
  }

  private String fullFieldName(String fieldName) {
    return tableName + "." + fieldName;
  }
}
