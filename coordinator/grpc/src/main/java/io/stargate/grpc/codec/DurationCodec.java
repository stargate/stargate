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

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DurationCodec implements ValueCodec {
  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.DURATION) {
      throw new IllegalArgumentException("Expected varint type");
    }
    QueryOuterClass.Duration duration = value.getDuration();
    int size =
        VIntCoding.computeVIntSize(duration.getMonths())
            + VIntCoding.computeVIntSize(duration.getDays())
            + VIntCoding.computeVIntSize(duration.getNanos());
    ByteArrayDataOutput out = ByteStreams.newDataOutput(size);
    try {
      VIntCoding.writeVInt(duration.getMonths(), out);
      VIntCoding.writeVInt(duration.getDays(), out);
      VIntCoding.writeVInt(duration.getNanos(), out);
    } catch (IOException e) {
      // cannot happen
      throw new AssertionError();
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    if (bytes.remaining() == 0) {
      throw new IllegalArgumentException(
          "Invalid duration value, expecting non-empty Bytes but got 0");
    }
    DataInput in = ByteStreams.newDataInput(getArray(bytes));
    try {
      int months = (int) VIntCoding.readVInt(in);
      int days = (int) VIntCoding.readVInt(in);
      long nanos = VIntCoding.readVInt(in);
      return Value.newBuilder()
          .setDuration(
              QueryOuterClass.Duration.newBuilder().setMonths(months).setDays(days).setNanos(nanos))
          .build();
    } catch (IOException e) {
      // cannot happen
      throw new AssertionError();
    }
  }

  private static byte[] getArray(ByteBuffer bytes) {
    int length = bytes.remaining();

    if (bytes.hasArray()) {
      int boff = bytes.arrayOffset() + bytes.position();
      if (boff == 0 && length == bytes.array().length) {
        return bytes.array();
      } else {
        return Arrays.copyOfRange(bytes.array(), boff, boff + length);
      }
    }
    // else, DirectByteBuffer.get() is the fastest route
    byte[] array = new byte[length];
    bytes.duplicate().get(array);
    return array;
  }
}
