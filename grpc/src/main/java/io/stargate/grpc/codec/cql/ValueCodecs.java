package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.util.EnumMap;

public class ValueCodecs {
  public static final EnumMap<Column.Type, ValueCodec> CODECS = new EnumMap<>(Column.Type.class);

  static {
    CODECS.put(Type.Ascii, new StringCodec(TypeCodecs.ASCII));
    CODECS.put(Type.Text, new StringCodec(TypeCodecs.TEXT));
    CODECS.put(Type.Varchar, new StringCodec(TypeCodecs.TEXT));
    CODECS.put(Type.Int, new IntCodec());
    CODECS.put(Type.Uuid, new UuidCodec(TypeCodecs.UUID));
    CODECS.put(Type.Timeuuid, new UuidCodec(TypeCodecs.TIMEUUID));
    CODECS.put(Type.Blob, new BytesCodec());
  }
}
