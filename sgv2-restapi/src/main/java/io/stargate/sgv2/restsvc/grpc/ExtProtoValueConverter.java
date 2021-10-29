package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter that knows how to convert a single row from the "external" Stargate Protobuf result
 * into a representation (currently {@link java.util.Map} with column name as key, Java value that
 * can be serialized by frontend framework (like DropWizard) as value).
 */
public class ExtProtoValueConverter {
  protected final String[] columnNames;
  protected final ExtProtoValueCodec[] codecs;

  protected ExtProtoValueConverter(String[] columnNames, ExtProtoValueCodec[] codecs) {
    this.columnNames = columnNames;
    this.codecs = codecs;
  }

  public static ExtProtoValueConverter construct(
      String[] columnNames, ExtProtoValueCodec[] codecs) {
    return new ExtProtoValueConverter(columnNames, codecs);
  }

  /**
   * Method called to convert External Protobuf values into Java value objects serializable by web
   * service such as DropWizard and other JAX-RS implementations.
   */
  public Map<String, Object> fromProtoValues(List<QueryOuterClass.Value> values) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (int i = 0, end = values.size(); i < end; ++i) {
      result.put(columnNames[i], codecs[i].fromProtoValue(values.get(i)));
    }
    return result;
  }
}
