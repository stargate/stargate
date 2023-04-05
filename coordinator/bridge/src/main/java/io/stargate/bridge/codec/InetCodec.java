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

import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.bridge.proto.QueryOuterClass.Inet;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.QueryOuterClass.Value.InnerCase;
import io.stargate.db.schema.Column.ColumnType;
import java.nio.ByteBuffer;

public class InetCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.INET) {
      throw new IllegalArgumentException("Expected bytes type");
    }
    ByteString address = value.getInet().getValue();
    int size = validateByteLength(address.size());
    ByteBuffer bytes = ByteBuffer.allocate(size);
    address.copyTo(bytes);
    bytes.flip();
    return bytes;
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    validateByteLength(bytes.remaining());
    return Value.newBuilder()
        .setInet(Inet.newBuilder().setValue(ByteString.copyFrom(bytes.duplicate())))
        .build();
  }

  private int validateByteLength(int byteLength) {
    if (byteLength != 4 && byteLength != 16) {
      throw new IllegalArgumentException(
          "Expected 4 or 16 bytes for an IPv4 or IPv6 address, got " + byteLength);
    }
    return byteLength;
  }
}
