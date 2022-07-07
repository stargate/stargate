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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

public class CollectionCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull Column.ColumnType type) {
    if (!value.hasCollection()) {
      throw new IllegalArgumentException("Expected collection type");
    }
    assert type.isCollection();

    Collection collection = value.getCollection();

    Column.ColumnType elementType = type.parameters().get(0);
    ValueCodec elementCodec = ValueCodecs.get(elementType.rawType());

    int elementCount = collection.getElementsCount();
    ByteBuffer[] encodedElements = new ByteBuffer[elementCount];
    int toAllocate = 4;
    for (int i = 0; i < elementCount; ++i) {
      ByteBuffer encodedElement =
          ValueCodec.encodeValue(elementCodec, collection.getElements(i), elementType);
      if (encodedElement == null) {
        throw new IllegalArgumentException(
            String.format("null is not supported inside %ss", type.rawType().cqlDefinition()));
      }
      encodedElements[i] = encodedElement;
      toAllocate += 4 + encodedElement.remaining();
    }

    ByteBuffer result = ByteBuffer.allocate(toAllocate);
    result.putInt(elementCount);
    return encodeValues(encodedElements, result);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    Collection.Builder builder = Collection.newBuilder();

    Column.ColumnType elementType = type.parameters().get(0);
    ValueCodec elementCodec = ValueCodecs.get(elementType.rawType());

    ByteBuffer input = bytes.duplicate();
    int elementCount = input.getInt();
    for (int i = 0; i < elementCount; i++) {
      builder.addElements(decodeValue(input, elementCodec, elementType));
    }

    return Value.newBuilder().setCollection(builder).build();
  }
}
