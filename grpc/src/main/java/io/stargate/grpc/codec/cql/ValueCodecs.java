package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class ValueCodecs {
  public static final Map<Type, ValueCodec> CODECS =
      Collections.unmodifiableMap(
          new EnumMap<Type, ValueCodec>(Column.Type.class) {
            {
              put(Type.Ascii, new StringCodec(TypeCodecs.ASCII));
              put(Type.Text, new StringCodec(TypeCodecs.TEXT));
              put(Type.Varchar, new StringCodec(TypeCodecs.TEXT));
              put(Type.Int, new IntCodec());
              put(Type.Uuid, new UuidCodec(TypeCodecs.UUID));
              put(Type.Timeuuid, new UuidCodec(TypeCodecs.TIMEUUID));
              put(Type.Blob, new BytesCodec());
            }
          });
}
