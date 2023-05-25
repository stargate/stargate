package org.apache.cassandra.stargate.transport.internal;

import io.netty.buffer.ByteBuf;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public class ColumnUtils {
  public static Column.ColumnType decodeType(ByteBuf src, ProtocolVersion version) {
    int id = src.readUnsignedShort();
    Column.Type type = Column.Type.fromId(id);
    if (type == null) {
      throw new ProtocolException(String.format("Invalid data type id %d in RESULT message", id));
    }
    switch (type) {
      case List:
        return Column.Type.List.of(decodeType(src, version));
      case Set:
        return Column.Type.Set.of(decodeType(src, version));
      case Map:
        Column.ColumnType kt = decodeType(src, version);
        Column.ColumnType vt = decodeType(src, version);
        return Column.Type.Map.of(kt, vt);
      case UDT:
        return decodeUDT(src, version);
      case Tuple:
        return decodeTuple(src, version);
      default:
        return type;
    }
  }

  private static Column.ColumnType decodeUDT(ByteBuf src, ProtocolVersion version) {
    ImmutableUserDefinedType.Builder builder = ImmutableUserDefinedType.builder();
    builder.keyspace(CBUtil.readString(src));
    builder.name(CBUtil.readString(src));

    int n = src.readUnsignedShort();
    List<Column> columns = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      ImmutableColumn.Builder columnBuilder = ImmutableColumn.builder();
      columnBuilder.name(CBUtil.readString(src));
      columnBuilder.type(decodeType(src, version));
      columns.add(columnBuilder.kind(Column.Kind.Regular).build());
    }
    return builder.columns(columns).build();
  }

  private static Column.ColumnType decodeTuple(ByteBuf src, ProtocolVersion version) {
    ImmutableTupleType.Builder builder = ImmutableTupleType.builder();
    int n = src.readUnsignedShort();
    for (int i = 0; i < n; ++i) builder.addParameters(decodeType(src, version));
    return builder.build();
  }

  public static Column decodeColumn(
      ByteBuf src, ProtocolVersion version, String keyspace, String table) {
    return ImmutableColumn.builder()
        .keyspace(keyspace)
        .table(table)
        .name(CBUtil.readString(src))
        .type(decodeType(src, version))
        .build();
  }

  public static void encodeType(Column.ColumnType type, ByteBuf dest, ProtocolVersion version) {
    dest.writeShort(type.id());
    switch (type.rawType()) {
      case List: // Fallthrough intended
      case Set:
        encodeType(type.parameters().get(0), dest, version);
        break;
      case Map:
        encodeType(type.parameters().get(0), dest, version);
        encodeType(type.parameters().get(1), dest, version);
        break;
      case UDT:
        UserDefinedType udt = (UserDefinedType) type;
        CBUtil.writeAsciiString(udt.keyspace(), dest);
        CBUtil.writeAsciiString(udt.name(), dest);
        dest.writeShort(udt.columns().size());
        for (Column c : udt.columns()) {
          CBUtil.writeAsciiString(c.name(), dest);
          encodeType(c.type(), dest, version);
        }
        break;
      case Tuple:
        ParameterizedType.TupleType tup = (ParameterizedType.TupleType) type;
        dest.writeShort(tup.parameters().size());
        for (Column.ColumnType t : tup.parameters()) {
          encodeType(t, dest, version);
        }
        break;
      case Composite: // Fallthrough intended
      case DynamicComposite: // Fallthrough intended
      case LineString: // Fallthrough intended
      case Point: // Fallthrough intended
      case Polygon: // Fallthrough intended
      case Vector:
        // For custom types the class name of the type follows the type ID (which is zero)
        CBUtil.writeAsciiString(type.marshalTypeName(), dest);
        break;
      default:
        // Nothing else to do for simple types
    }
  }

  public static int encodeSizeType(Column.ColumnType type, ProtocolVersion version) {
    int size = 2;
    switch (type.rawType()) {
      case List: // Fallthrough intended
      case Set:
        size += encodeSizeType(type.parameters().get(0), version);
        break;
      case Map:
        size += encodeSizeType(type.parameters().get(0), version);
        size += encodeSizeType(type.parameters().get(1), version);
        break;
      case UDT:
        UserDefinedType udt = (UserDefinedType) type;
        size += CBUtil.sizeOfAsciiString(udt.keyspace());
        size += CBUtil.sizeOfAsciiString(udt.name());
        size += 2;
        for (Column c : udt.columns()) {
          size += CBUtil.sizeOfAsciiString(c.name());
          size += encodeSizeType(c.type(), version);
        }
        break;
      case Tuple:
        ParameterizedType.TupleType tup = (ParameterizedType.TupleType) type;
        size += 2;
        for (Column.ColumnType t : tup.parameters()) {
          size += encodeSizeType(t, version);
        }
        break;
      case Composite: // Fallthrough intended
      case DynamicComposite: // Fallthrough intended
      case LineString: // Fallthrough intended
      case Point: // Fallthrough intended
      case Polygon: // Fallthrough intended
      case Vector:
        size += CBUtil.sizeOfAsciiString(type.marshalTypeName());
        break;
      default: // fall though (simple types)
    }
    return size;
  }

  public static void encodeColumn(Column column, ByteBuf dest, ProtocolVersion version) {
    CBUtil.writeAsciiString(column.name(), dest);
    encodeType(column.type(), dest, version);
  }

  public static int encodeSizeColumn(Column column, ProtocolVersion version) {
    return CBUtil.sizeOfAsciiString(column.name()) + encodeSizeType(column.type(), version);
  }
}
