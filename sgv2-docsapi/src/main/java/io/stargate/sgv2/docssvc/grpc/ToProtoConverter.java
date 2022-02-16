package io.stargate.sgv2.docssvc.grpc;

import io.stargate.grpc.Values;
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
    if (value == null) {
      return Values.NULL;
    }
    final ToProtoValueCodec codec = getCodec(fieldName);
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
    if (value == null) {
      return Values.NULL;
    }
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
    final ToProtoValueCodec codec = getCodec(fieldName);
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

  /**
   * Specialized method that will try to find Field codec similar to {@link
   * #protoValueFromLooselyTyped} (failing with exception if no such field found), but if one found:
   *
   * <ul>
   *   <li>If field has Map type, use its Key codec for converting value
   *   <li>Return {@code null} if field is NOT of Map type
   * </ul>
   *
   * @param fieldName Name of field to find
   * @param value Loosely typed value to convert to Map key type
   * @return Converted value if field is of Map type and conversion succeeds; {@code null} if field
   *     is of non-Map type.
   */
  public QueryOuterClass.Value keyProtoValueFromLooselyTyped(String fieldName, Object value) {
    final ToProtoValueCodec mainCodec = getCodec(fieldName);
    final ToProtoValueCodec keyCodec = mainCodec.getKeyCodec();

    // Non-container type? If so, return null so caller can indicate problem
    if (keyCodec == null) {
      return null;
    }
    try {
      if (value instanceof String) {
        // Need to allow optional use of "extra" single quotes
        String strValue = StringifiedValueUtil.handleSingleQuotes((String) value);
        return keyCodec.protoValueFromStringified(strValue);
      }
      return keyCodec.protoValueFromStrictlyTyped(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to deserialize key value for field '%s', problem: %s",
              fullFieldName(fieldName), e.getMessage()),
          e);
    }
  }

  /**
   * Specialized method that will try to find Field codec similar to {@link
   * #protoValueFromLooselyTyped} (failing with exception if no such field found), but if one found:
   *
   * <ul>
   *   <li>If field has Container type (set, list, map), use its "content" / value codec for
   *       converting value
   *   <li>Return {@code null} if field is NOT of Container type
   * </ul>
   *
   * @param fieldName Name of field to find
   * @param value Loosely typed value to convert to Container content type
   * @return Converted value if field is of Container type and conversion succeeds; {@code null} if
   *     field is of non-Container type.
   */
  public QueryOuterClass.Value contentProtoValueFromLooselyTyped(String fieldName, Object value) {
    final ToProtoValueCodec mainCodec = getCodec(fieldName);
    final ToProtoValueCodec valueCodec = mainCodec.getValueCodec();

    // Non-container type? If so, return null so caller can indicate problem
    if (valueCodec == null) {
      return null;
    }
    try {
      if (value instanceof String) {
        // Need to allow optional use of "extra" single quotes
        String strValue = StringifiedValueUtil.handleSingleQuotes((String) value);
        return valueCodec.protoValueFromStringified(strValue);
      }
      return valueCodec.protoValueFromStrictlyTyped(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to deserialize content value for field '%s', problem: %s",
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
