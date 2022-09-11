package io.stargate.sgv2.dynamosvc.grpc;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.bridge.proto.QueryOuterClass;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter that knows how to convert a single row from the "Bridge" Stargate Protobuf result into
 * a representation (currently {@link Map} with column name as key, Java value that can be
 * serialized by frontend framework (like DropWizard) as value, usually as JSON.
 */
public class FromProtoConverter {
  static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

  protected final String[] columnNames;
  protected final FromProtoValueCodec[] codecs;

  protected FromProtoConverter(String[] columnNames, FromProtoValueCodec[] codecs) {
    this.columnNames = columnNames;
    this.codecs = codecs;
  }

  /**
   * Factory method for constructing converter instances, given ordered sets of column names and
   * matching {@link FromProtoValueCodec}s.
   */
  public static FromProtoConverter construct(String[] columnNames, FromProtoValueCodec[] codecs) {
    return new FromProtoConverter(columnNames, codecs);
  }

  /**
   * Method called to convert Bridge Protobuf values into Java value objects serializable by web
   * service such as DropWizard and other JAX-RS implementations.
   */
  public Map<String, Object> mapFromProtoValues(List<QueryOuterClass.Value> values) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (int i = 0, end = values.size(); i < end; ++i) {
      try {
        result.put(columnNames[i], codecs[i].fromProtoValue(values.get(i)));
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "Internal error: failed to convert value of column #%d/#%d ('%s'), problem: %s",
                i + 1, end, columnNames[i], e.getMessage()),
            e);
      }
    }
    return result;
  }

  /**
   * Method called to convert Bridge Protobuf values into {@link ObjectNode}s: node values are
   * easier to manipulate than "simple" Java {@code java.lang.Object} values, and also as
   * serializable by web service such as DropWizard and other JAX-RS implementations.
   */
  public ObjectNode objectNodeFromProtoValues(List<QueryOuterClass.Value> values) {
    ObjectNode result = jsonNodeFactory.objectNode();
    for (int i = 0, end = values.size(); i < end; ++i) {
      try {
        result.set(columnNames[i], codecs[i].jsonNodeFrom(values.get(i)));
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "Internal error: failed to convert value of column #%d/#%d ('%s'), problem: %s",
                i + 1, end, columnNames[i], e.getMessage()),
            e);
      }
    }
    return result;
  }
}
