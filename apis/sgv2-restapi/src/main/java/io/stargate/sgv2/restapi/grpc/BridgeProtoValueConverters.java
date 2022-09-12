package io.stargate.sgv2.restapi.grpc;

import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for constructing converters to convert between (external) gPRC/Proto {@code Value}s and
 * "Java" values like {@link Map}s and {@code JsonNode}s (Jackson type).
 *
 * <p>Currently supported:
 *
 * <ul>
 *   <li>{@link FromProtoConverter} converts from "external" Stargate Protobuf column values into
 *       "Java" values
 * </ul>
 */
public class BridgeProtoValueConverters {
  private static final FromProtoValueCodecs FROM_PROTO_CODECS = new FromProtoValueCodecs();
  private static final ToProtoValueCodecs TO_PROTO_CODECS = new ToProtoValueCodecs();

  private static final BridgeProtoValueConverters INSTANCE = new BridgeProtoValueConverters();

  public static BridgeProtoValueConverters instance() {
    return INSTANCE;
  }

  public FromProtoConverter fromProtoConverter(List<QueryOuterClass.ColumnSpec> columns) {
    final String[] names = new String[columns.size()];
    final FromProtoValueCodec[] codecs = new FromProtoValueCodec[columns.size()];

    for (int i = 0, end = columns.size(); i < end; ++i) {
      QueryOuterClass.ColumnSpec spec = columns.get(i);
      names[i] = spec.getName();
      try {
        codecs[i] = FROM_PROTO_CODECS.codecFor(spec);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Internal problem: failed to create converter for column #%d/%d ('%s'): %s",
                i + 1, end, spec.getName(), e.getMessage()));
      }
    }
    return FromProtoConverter.construct(names, codecs);
  }

  /** Factory method that will fetch converters for all fields. */
  public ToProtoConverter toProtoConverter(Schema.CqlTable forTable) {
    // retain order for error message info
    Map<String, ToProtoValueCodec> codecsByName = new LinkedHashMap<>();
    addFields(forTable, codecsByName, forTable.getPartitionKeyColumnsList());
    addFields(forTable, codecsByName, forTable.getClusteringKeyColumnsList());
    addFields(forTable, codecsByName, forTable.getStaticColumnsList());
    addFields(forTable, codecsByName, forTable.getColumnsList());
    return new ToProtoConverter(forTable.getName(), codecsByName);
  }

  private static void addFields(
      Schema.CqlTable tableDef,
      Map<String, ToProtoValueCodec> codecsByName,
      List<QueryOuterClass.ColumnSpec> columns) {
    for (QueryOuterClass.ColumnSpec column : columns) {
      try {
        codecsByName.put(column.getName(), TO_PROTO_CODECS.codecFor(column));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to create codec for field '%s' (table '%s'), problem: %s",
                column.getName(), tableDef.getName(), e.getMessage()),
            e);
      }
    }
  }
}
