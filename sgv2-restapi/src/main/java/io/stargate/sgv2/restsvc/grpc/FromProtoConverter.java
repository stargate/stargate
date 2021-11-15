package io.stargate.sgv2.restsvc.grpc;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.proto.QueryOuterClass;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter that knows how to convert a single row from the "external" Stargate Protobuf result
 * into a representation (currently {@link java.util.Map} with column name as key, Java value that
 * can be serialized by frontend framework (like DropWizard) as value, usually as JSON.
 */
public class FromProtoConverter {
  static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

  protected final String[] columnNames;
  protected final FromProtoValueCodec[] codecs;

  protected FromProtoConverter(String[] columnNames, FromProtoValueCodec[] codecs) {
    this.columnNames = columnNames;
    this.codecs = codecs;
  }

  public static FromProtoConverter construct(String[] columnNames, FromProtoValueCodec[] codecs) {
    return new FromProtoConverter(columnNames, codecs);
  }

  /**
   * Method called to convert External Protobuf values into Java value objects serializable by web
   * service such as DropWizard and other JAX-RS implementations.
   */
  public Map<String, Object> mapFromProtoValues(List<QueryOuterClass.Value> values) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (int i = 0, end = values.size(); i < end; ++i) {
      result.put(columnNames[i], codecs[i].fromProtoValue(values.get(i)));
    }
    return result;
  }

  public ObjectNode objectNodeFromProtoValues(List<QueryOuterClass.Value> values) {
    ObjectNode result = jsonNodeFactory.objectNode();
    for (int i = 0, end = values.size(); i < end; ++i) {
      result.set(columnNames[i], codecs[i].jsonNodeFrom(values.get(i)));
    }
    return result;
  }
}
