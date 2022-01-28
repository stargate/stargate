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
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class UuidCodec implements ValueCodec {
  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull Column.ColumnType type) {
    if (value.getInnerCase() != InnerCase.UUID) {
      throw new IllegalArgumentException("Expected UUID type");
    }
    ByteString uuid = value.getUuid().getValue();
    int size = validateByteLength(uuid.size());
    ByteBuffer bytes = ByteBuffer.allocate(size);
    uuid.copyTo(bytes);
    bytes.flip();
    return bytes;
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    validateByteLength(bytes.remaining());
    return Value.newBuilder()
        .setUuid(Uuid.newBuilder().setValue(ByteString.copyFrom(bytes.duplicate())).build())
        .build();
  }

  private int validateByteLength(int byteLength) {
    if (byteLength != 16) {
      throw new IllegalArgumentException("Expected 16 bytes for a UUID, got " + byteLength);
    }
    return byteLength;
  }
}
