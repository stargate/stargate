package io.stargate.grpc.codec.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.UdtValue;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class UdtCodec extends CompositeCodec {

  @Override
  public ByteBuffer encode(@NonNull QueryOuterClass.Value value, @NonNull Column.ColumnType type) {
    if (!value.hasUdt()) {
      throw new IllegalArgumentException("Expected user-defined type");
    }
    assert type.isUserDefined();

    UdtValue udtValue = value.getUdt();
    Map<String, Value> udtFieldsValues = udtValue.getFieldsMap();

    Map<String, Column> fields = ((UserDefinedType) type).columnMap();
    for (String name : udtFieldsValues.keySet()) {
      if (!fields.containsKey(name)) {
        throw new IllegalArgumentException(
            String.format("User-defined type doesn't contain a field named '%s'", name));
      }
    }

    int fieldsCount = fields.size();
    ByteBuffer[] encodedFields = new ByteBuffer[fieldsCount];

    int i = 0;
    int toAllocate = 0;
    for (Column field : fields.values()) {
      ByteBuffer encodedField = null;
      Value fieldValue = udtFieldsValues.get(field.name());
      if (fieldValue != null) {
        Column.ColumnType fieldType = Objects.requireNonNull(field.type());
        ValueCodec fieldCodec = ValueCodecs.get(fieldType.rawType());
        encodedField = ValueCodec.encodeValue(fieldCodec, fieldValue, fieldType);
      }
      toAllocate += 4 + (encodedField == null ? 0 : encodedField.remaining());
      encodedFields[i++] = encodedField;
    }

    return encodeValues(encodedFields, ByteBuffer.allocate(toAllocate));
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull Column.ColumnType type) {
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
