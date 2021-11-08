package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.List;

/**
 * Factory for constructing {@link ExtProtoValueConverter} from "external" Stargate Protobuf column
 * definitions.
 */
public class ExtProtoValueConverters {
  private static final ExtProtoValueCodecs CODECS = new ExtProtoValueCodecs();

  private static final ExtProtoValueConverters INSTANCE = new ExtProtoValueConverters();

  public static ExtProtoValueConverters instance() {
    return INSTANCE;
  }

  public ExtProtoValueConverter createConverter(List<QueryOuterClass.ColumnSpec> columns) {
    final String[] names = new String[columns.size()];
    final ExtProtoValueCodec[] codecs = new ExtProtoValueCodec[columns.size()];

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
    return ExtProtoValueConverter.construct(names, codecs);
  }
}
