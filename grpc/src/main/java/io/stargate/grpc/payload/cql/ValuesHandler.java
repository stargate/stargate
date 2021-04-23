package io.stargate.grpc.payload.cql;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.grpc.Status;
import io.grpc.StatusException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.grpc.codec.cql.ValueCodec;
import io.stargate.grpc.codec.cql.ValueCodecs;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.proto.QueryOuterClass.*;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.TypeSpec.Builder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ValuesHandler implements PayloadHandler {
  private static final Value NULL_VALUE =
      Value.newBuilder().setNull(Value.Null.newBuilder().build()).build();

  @Override
  public BoundStatement bindValues(Prepared prepared, Payload payload, ByteBuffer unsetValue)
      throws InvalidProtocolBufferException, StatusException {
    final Values values = payload.getValue().unpack(Values.class);
    final List<Column> columns = prepared.metadata.columns;
    final int columnCount = columns.size();
    final int valuesCount = values.getValuesCount();
    if ((columnCount > 0 && values == null) || (values != null && columnCount != valuesCount)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Invalid number of bind values. Expected %d, but received %d",
                  columnCount, values == null ? 0 : valuesCount))
          .asException();
    }
    final List<ByteBuffer> boundValues = new ArrayList<>(columnCount);
    List<String> boundValueNames = null;
    if (values != null && values.getValueNamesCount() != 0) {
      final int namesCount = values.getValueNamesCount();
      if (namesCount != columnCount) {
        throw Status.FAILED_PRECONDITION
            .withDescription(
                String.format(
                    "Invalid number of bind names. Expected %d, but received %d",
                    columnCount, namesCount))
            .asException();
      }
      boundValueNames = new ArrayList<>(namesCount);
      for (int i = 0; i < namesCount; ++i) {
        String name = values.getValueNames(i);
        Column column =
            columns.stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElseThrow(
                    () ->
                        Status.INVALID_ARGUMENT
                            .withDescription(
                                String.format("Unable to find bind marker with name '%s'", name))
                            .asException());
        ColumnType columnType = columnTypeNotNull(column);
        ValueCodec codec = ValueCodecs.CODECS.get(columnType.rawType());
        Value value = values.getValues(i);
        if (codec == null) {
          throw Status.UNIMPLEMENTED
              .withDescription(String.format("Unsupported type %s", columnType))
              .asException();
        }
        try {
          boundValues.add(encodeValue(codec, value, columnType, unsetValue));
        } catch (Exception e) {
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Invalid argument for name '%s'", name))
              .withCause(e)
              .asException();
        }
        boundValueNames.add(name);
      }
    } else {
      for (int i = 0; i < columnCount; ++i) {
        Column column = columns.get(i);
        Value value = values.getValues(i);
        ColumnType columnType = columnTypeNotNull(column);
        ValueCodec codec = ValueCodecs.CODECS.get(columnType.rawType());
        if (codec == null) {
          throw Status.UNIMPLEMENTED
              .withDescription(String.format("Unsupported type %s", columnType))
              .asException();
        }
        try {
          boundValues.add(encodeValue(codec, value, columnType, unsetValue));
        } catch (Exception e) {
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Invalid argument at position %d", i + 1))
              .withCause(e)
              .asException();
        }
      }
    }

    return new BoundStatement(prepared.statementId, boundValues, boundValueNames);
  }

  @Override
  public Payload processResult(Rows rows, QueryParameters parameters) throws StatusException {
    Payload.Builder payloadBuilder = Payload.newBuilder().setType(Type.TYPE_CQL);

    final List<Column> columns = rows.resultMetadata.columns;
    final int columnCount = columns.size();

    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    if (!parameters.getSkipMetadata()) {
      for (Column column : columns) {
        resultSetBuilder.addColumns(
            ColumnSpec.newBuilder()
                .setType(convertType(columnTypeNotNull(column)))
                .setName(column.name())
                .build());
      }
    }

    for (List<ByteBuffer> row : rows.rows) {
      Row.Builder rowBuilder = Row.newBuilder();
      for (int i = 0; i < columnCount; ++i) {
        ColumnType columnType = columnTypeNotNull(columns.get(i));
        ValueCodec codec = ValueCodecs.CODECS.get(columnType.rawType());
        if (codec == null) {
          throw Status.FAILED_PRECONDITION.withDescription("Unsupported column type").asException();
        }
        rowBuilder.addValues(decodeValue(codec, row.get(i)));
      }
      resultSetBuilder.addRows(rowBuilder);
    }
    return payloadBuilder.setValue(Any.pack(resultSetBuilder.build())).build();
  }

  @Nullable
  private ByteBuffer encodeValue(
      ValueCodec codec, Value value, ColumnType columnType, ByteBuffer unsetValue) {
    if (value.hasNull()) {
      return null;
    } else if (value.hasUnset()) {
      return unsetValue;
    } else {
      return codec.encode(value, columnType);
    }
  }

  private Value decodeValue(ValueCodec codec, ByteBuffer bytes) {
    if (bytes == null) {
      return NULL_VALUE;
    } else {
      return codec.decode(bytes);
    }
  }

  @NonNull
  private ColumnType columnTypeNotNull(Column column) throws StatusException {
    ColumnType type = column.type();
    if (type == null) {
      throw Status.INTERNAL
          .withDescription(String.format("Column '%s' doesn't have a valid type", column.name()))
          .asException();
    }
    return type;
  }

  private TypeSpec convertType(ColumnType columnType) throws StatusException {
    Builder builder = TypeSpec.newBuilder().setType(TypeSpec.Type.forNumber(columnType.id()));

    if (columnType.isParameterized()) {
      List<ColumnType> parameters = columnType.parameters();

      switch (columnType.rawType()) {
        case List:
          if (parameters.size() != 1) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Expected list type to have a parameterized type")
                .asException();
          }
          builder.setList(ListSpec.newBuilder().setElement(convertType(parameters.get(0))).build());
          break;
        case Map:
          if (parameters.size() != 2) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Expected map type to have key/value parameterized types")
                .asException();
          }
          builder.setMap(
              MapSpec.newBuilder()
                  .setKey(convertType(parameters.get(0)))
                  .setValue(convertType(parameters.get(1)))
                  .build());
          break;
        case Set:
          if (parameters.size() != 1) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Expected set type to have a parameterized type")
                .asException();
          }
          builder.setSet(SetSpec.newBuilder().setElement(convertType(parameters.get(0))).build());
          break;
        case Tuple:
          if (parameters.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Expected tuple type to have at least one parameterized type")
                .asException();
          }
          TupleSpec.Builder tupleBuilder = TupleSpec.newBuilder();
          for (ColumnType parameter : parameters) {
            tupleBuilder.addElements(convertType(parameter));
          }
          builder.setTuple(tupleBuilder.build());
          break;
        case UDT:
          UserDefinedType udt = (UserDefinedType) columnType;
          if (udt.columns().isEmpty()) {
            throw Status.FAILED_PRECONDITION
                .withDescription("Expected user defined type to have at least one field")
                .asException();
          }
          UdtSpec.Builder udtBuilder = UdtSpec.newBuilder();
          for (Column column : udt.columns()) {
            udtBuilder.putFields(column.name(), convertType(columnTypeNotNull(column).rawType()));
          }
          builder.setUdt(udtBuilder.build());
          break;
        default:
          throw new AssertionError("Unhandled parameterized type");
      }
    }

    return builder.build();
  }
}
