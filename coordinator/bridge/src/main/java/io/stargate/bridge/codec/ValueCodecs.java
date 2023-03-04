/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.bridge.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.stargate.db.schema.Column.Type;

public class ValueCodecs {

  private static final ImmutableMap<Type, ValueCodec> CODECS =
      Maps.immutableEnumMap(
          ImmutableMap.<Type, ValueCodec>builder()
              .put(Type.Ascii, new StringCodec(TypeCodecs.ASCII))
              .put(Type.Bigint, new BigintCodec())
              .put(Type.Blob, new BytesCodec())
              .put(Type.Boolean, new BooleanCodec())
              .put(Type.Counter, new BigintCodec())
              .put(Type.Date, new DateCodec())
              .put(Type.Decimal, new DecimalCodec())
              .put(Type.Double, new DoubleCodec())
              .put(Type.Duration, new DurationCodec())
              .put(Type.Float, new FloatCodec())
              .put(Type.Int, new IntCodec())
              .put(Type.Inet, new InetCodec())
              .put(Type.Smallint, new SmallintCodec())
              .put(Type.Text, new StringCodec(TypeCodecs.TEXT))
              .put(Type.Time, new TimeCodec())
              .put(Type.Timestamp, new BigintCodec())
              .put(Type.Timeuuid, new UuidCodec())
              .put(Type.Tinyint, new TinyintCodec())
              .put(Type.Uuid, new UuidCodec())
              .put(Type.Varint, new VarintCodec())
              .put(Type.List, new CollectionCodec())
              .put(Type.Set, new CollectionCodec())
              .put(Type.Map, new MapCodec())
              .put(Type.Tuple, new TupleCodec())
              .put(Type.UDT, new UdtCodec())
              .build());

  @NonNull
  public static ValueCodec get(Type type) {
    ValueCodec codec = CODECS.get(type);
    if (codec == null) {
      throw Status.UNIMPLEMENTED
          .withDescription(String.format("Unable to encode/decode type '%s'", type.cqlDefinition()))
          .asRuntimeException();
    }
    return codec;
  }
}
