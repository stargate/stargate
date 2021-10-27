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

import io.stargate.db.schema.Column;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

public abstract class CompositeCodec implements ValueCodec {

  protected Value decodeValue(ByteBuffer bytes, ValueCodec codec, Column.ColumnType type) {
    Value element;
    int elementSize = bytes.getInt();
    if (elementSize < 0) {
      element = Values.NULL;
    } else {
      ByteBuffer encodedElement = bytes.slice();
      encodedElement.limit(elementSize);
      element = ValueCodec.decodeValue(codec, encodedElement, type);
      bytes.position(bytes.position() + elementSize);
    }
    return element;
  }

  protected ByteBuffer encodeValues(ByteBuffer[] values, ByteBuffer result) {
    for (ByteBuffer value : values) {
      if (value == null) {
        result.putInt(-1);
      } else {
        result.putInt(value.remaining());
        result.put(value);
      }
    }
    result.flip();
    return result;
  }
}
