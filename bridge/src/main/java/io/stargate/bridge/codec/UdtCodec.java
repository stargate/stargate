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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.bridge.proto.QueryOuterClass.UdtValue;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.UserDefinedType;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UdtCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (!value.hasUdt()) {
      throw new IllegalArgumentException("Expected user-defined type");
    }
    assert type.isUserDefined();

    UdtValue udtValue = value.getUdt();
    Map<String, Value> udtFieldsValues = udtValue.getFieldsMap();

    Map<String, Column> fieldsByName = ((UserDefinedType) type).columnMap();
    for (String name : udtFieldsValues.keySet()) {
      if (!fieldsByName.containsKey(name)) {
        throw new IllegalArgumentException(
            String.format("User-defined type doesn't contain a field named '%s'", name));
      }
    }

    List<Column> fields = ((UserDefinedType) type).columns();
    int fieldsCount = fields.size();
    ByteBuffer[] encodedFields = new ByteBuffer[fieldsCount];

    int i = 0;
    int toAllocate = 0;
    for (Column field : fields) { // can't use fieldsByName.values() here, because the order matters
      ByteBuffer encodedField = null;
      Value fieldValue = udtFieldsValues.get(field.name());
      if (fieldValue != null) {
        ColumnType fieldType = Objects.requireNonNull(field.type());
        ValueCodec fieldCodec = ValueCodecs.get(fieldType.rawType());
        encodedField = ValueCodec.encodeValue(fieldCodec, fieldValue, fieldType);
      }
      toAllocate += 4 + (encodedField == null ? 0 : encodedField.remaining());
      encodedFields[i++] = encodedField;
    }

    return encodeValues(encodedFields, ByteBuffer.allocate(toAllocate));
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    UdtValue.Builder builder = UdtValue.newBuilder();

    assert type.isUserDefined();

    try {
      ByteBuffer input = bytes.duplicate();
      List<Column> udtFields = ((UserDefinedType) type).columns();
      int fieldCount = udtFields.size();
      int i = 0;
      while (input.hasRemaining()) {
        if (i > fieldCount) {
          throw new IllegalArgumentException(
              String.format("Too many fields in encoded UDT, expected %d", fieldCount));
        }
        String fieldName = udtFields.get(i).name();
        ColumnType fieldType = Objects.requireNonNull(udtFields.get(i).type());
        ValueCodec fieldCodec = ValueCodecs.get(fieldType.rawType());
        builder.putFields(fieldName, decodeValue(input, fieldCodec, fieldType));
        i++;
      }

      return Value.newBuilder().setUdt(builder).build();
    } catch (BufferUnderflowException e) {
      throw new IllegalArgumentException("Not enough bytes to deserialize a UDT", e);
    }
  }
}
