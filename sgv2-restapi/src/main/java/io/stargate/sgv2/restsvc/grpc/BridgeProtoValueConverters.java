package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.List;

/**
 * Factory for constructing converters to convert between (external) gPRC/Proto {@code Value}s and
 * "Java" values like {@link java.util.Map}s and {@code JsonNode}s (Jackson type).
 *
 * <p>Currently supported:
 *
 * <ul>
 *   <li>{@link FromProtoConverter} converts from "external" Stargate Protobuf column values into
 *       "Java" values
 * </ul>
 */
public class BridgeProtoValueConverters {
  private static final FromProtoValueCodecs CODECS = new FromProtoValueCodecs();

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
        codecs[i] = CODECS.codecFor(spec);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Internal problem: failed to create converter for column #%d/%d ('%s'): %s",
                i + 1, end, spec.getName(), e.getMessage()));
      }
    }
    return FromProtoConverter.construct(names, codecs);
  }
}
