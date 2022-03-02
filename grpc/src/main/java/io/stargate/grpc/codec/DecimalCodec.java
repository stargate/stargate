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
package io.stargate.grpc.codec;

import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class DecimalCodec implements ValueCodec {
  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.DECIMAL) {
      throw new IllegalArgumentException("Expected decimal type");
    }
    QueryOuterClass.Decimal decimal = value.getDecimal();
    ByteString bi = decimal.getValue();
    int scale = decimal.getScale();
    byte[] bibytes = bi.toByteArray();

    ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
    bytes.putInt(scale);
    bytes.put(bibytes);
    bytes.rewind();
    return bytes;
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    if (bytes.remaining() < 4) {
      throw new IllegalArgumentException(
          "Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());
    }

    bytes = bytes.duplicate();
    int scale = bytes.getInt();
    byte[] bibytes = new byte[bytes.remaining()];
    bytes.get(bibytes);
    return Value.newBuilder()
        .setDecimal(
            QueryOuterClass.Decimal.newBuilder()
                .setValue(ByteString.copyFrom(bibytes))
                .setScale(scale)
                .build())
        .build();
  }
}
