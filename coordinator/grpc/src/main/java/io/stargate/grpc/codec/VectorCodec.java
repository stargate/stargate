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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class VectorCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.hasNull()) return null;
    if (!value.hasCollection()) {
      throw new IllegalArgumentException("Expected collection of float type");
    }
    Collection collection = value.getCollection();
    int elementCount = collection.getElementsCount();

    int vectorSize = type.size();

    if (elementCount != vectorSize) {
      throw new IllegalArgumentException(
          String.format("Expected vector of size %d, but received %d", vectorSize, elementCount));
    }

    ByteBuffer[] encodedElements = new ByteBuffer[elementCount];
    int toAllocate = 0;
    for (int i = 0; i < elementCount; ++i) {
      final Value element = collection.getElements(i);

      if (element.getInnerCase().getNumber() != Value.InnerCase.FLOAT.getNumber()) {
        throw new IllegalArgumentException("Expected collection of float type");
      }
      final float floatValue = element.getFloat();
      ByteBuffer encodedElement = TypeCodecs.FLOAT.encode(floatValue, PROTOCOL_VERSION);
      if (encodedElement == null) {
        throw new IllegalArgumentException(
            String.format("null is not supported inside %ss", type.rawType().cqlDefinition()));
      }
      toAllocate += encodedElement.limit();
      encodedElement.rewind();
      encodedElements[i] = encodedElement;
    }

    ByteBuffer result = ByteBuffer.allocate(toAllocate);
    for (int i = 0; i < elementCount; ++i) {
      result.put(encodedElements[i]);
    }
    result.flip();
    return result;
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    if (bytes == null || bytes.remaining() == 0) {
      return Values.NULL;
    }
    int elementCount =
        Math.floorDiv(bytes.remaining(), Float.BYTES); // 4 represents 4 bytes for float
    ColumnType elementType = Column.Type.Float;
    ValueCodec elementCodec = ValueCodecs.get(elementType.rawType());
    List<Value> res = new ArrayList<>(elementCount);
    for (int i = 0; i < elementCount; ++i) {
      ByteBuffer slice = bytes.slice();
      slice.limit(Float.BYTES);
      res.add(elementCodec.decode(slice, Column.Type.Float));
      bytes.position(bytes.position() + Float.BYTES);
    }
    /* Restore the input ByteBuffer to its original state */
    bytes.rewind();

    return Values.of(res);
  }
}
