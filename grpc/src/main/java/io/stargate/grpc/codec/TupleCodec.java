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
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class TupleCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (!value.hasCollection()) {
      throw new IllegalArgumentException("Expected collection type");
    }
    assert type.isTuple();

    Collection tuple = value.getCollection();

    int fieldCount = tuple.getElementsCount();
    if (fieldCount > type.parameters().size()) {
      throw new IllegalArgumentException(
          String.format(
              "Too many tuple fields. Expected %d, but received %d",
              type.parameters().size(), fieldCount));
    }

    ByteBuffer[] encodedFields = new ByteBuffer[fieldCount];
    int toAllocate = 4;
    for (int i = 0; i < fieldCount; ++i) {
      ColumnType fieldType = type.parameters().get(i);
      ValueCodec fieldCodec = ValueCodecs.get(fieldType.rawType());

      ByteBuffer encodedField = ValueCodec.encodeValue(fieldCodec, tuple.getElements(i), fieldType);
      encodedFields[i] = encodedField;
      toAllocate += 4 + (encodedField == null ? 0 : encodedField.remaining());
    }

    return encodeValues(encodedFields, ByteBuffer.allocate(toAllocate));
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    Collection.Builder builder = Collection.newBuilder();

    try {
      ByteBuffer input = bytes.duplicate();
      int fieldCount = type.parameters().size();
      int i = 0;
      while (input.hasRemaining()) {
        if (i > fieldCount) {
          throw new IllegalArgumentException(
              String.format("Too many fields in encoded tuple, expected %d", fieldCount));
        }
        ColumnType fieldType = type.parameters().get(i);
        ValueCodec fieldCodec = ValueCodecs.get(fieldType.rawType());
        builder.addElements(decodeValue(input, fieldCodec, fieldType));
        i++;
      }

      return Value.newBuilder().setCollection(builder).build();
    } catch (BufferUnderflowException e) {
      throw new IllegalArgumentException("Not enough bytes to deserialize a tuple", e);
    }
  }
}
