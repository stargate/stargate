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
  }
}
