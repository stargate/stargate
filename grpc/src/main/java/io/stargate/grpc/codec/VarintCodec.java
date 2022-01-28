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
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import io.stargate.proto.QueryOuterClass.Varint;
import java.nio.ByteBuffer;

public class VarintCodec implements ValueCodec {
  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.VARINT) {
      throw new IllegalArgumentException("Expected varint type");
    }
    Varint varint = value.getVarint();
    return ByteBuffer.wrap(varint.getValue().toByteArray());
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    if (bytes.remaining() == 0) {
      return null;
    }

    return Value.newBuilder()
        .setVarint(Varint.newBuilder().setValue(ByteString.copyFrom(bytes.duplicate())).build())
        .build();
  }
}
