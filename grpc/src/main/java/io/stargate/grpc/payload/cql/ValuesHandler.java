package io.stargate.grpc.payload.cql;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
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
  @Override
  public BoundStatement bindValues(Prepared prepared, Payload payload)
      throws InvalidProtocolBufferException, StatusException {
    final Values values = payload.getValue().unpack(Values.class);
    final List<Column> columns = prepared.metadata.columns;
    final int columnCount = columns.size();
    if (values == null && columnCount > 0 || columnCount != values.getValuesCount()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Invalid number of bind parameters. Expected %d, but received %d",
                  columnCount, values == null ? 0 : values.getValuesCount()))
          .asException();
    }
    final List<ByteBuffer> boundValues = new ArrayList<>(columnCount);
    List<String> boundValueNames = null;
    if (values.getValueNamesCount() != 0) {
      final int namesCount = values.getValueNamesCount();
      boundValueNames = new ArrayList<>(namesCount);
      for (int i = 0; i < namesCount; ++i) {
        String name = values.getValueNames(i);
        Column column =
            columns.stream()
                .filter(c -> c.name() == name)
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException("Unable to find bind marker with name"));
        ValueCodec codec = ValueCodecs.CODECS.get(column.type());
        Value value = values.getValues(i);
        if (codec == null) {
          throw Status.UNIMPLEMENTED
              .withDescription(String.format("Unsupported type %s", column.type()))
              .asException();
        }
        boundValues.add(codec.encode(value, column.type()));
        boundValueNames.add(name);
      }
    } else {
      for (int i = 0; i < columnCount; ++i) {
        Column column = columns.get(i);
        Value value = values.getValues(i);
        ValueCodec codec = ValueCodecs.CODECS.get(column.type());
        if (codec == null) {
          throw Status.UNIMPLEMENTED
              .withDescription(String.format("Unsupported type %s", column.type()))
              .asException();
        }
        boundValues.add(codec.encode(value, column.type()));
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
        ColumnSpec.newBuilder().setType(convertType(column.type())).setName(column.name());
      }
    }

    for (List<ByteBuffer> row : rows.rows) {
      for (int i = 0; i < columnCount; ++i) {
        Column column = columns.get(i);
        ValueCodec codec = ValueCodecs.CODECS.get(column.type());
        if (codec == null) {
          throw Status.FAILED_PRECONDITION.withDescription("Unsupported column type").asException();
        }
        resultSetBuilder.addRows(Row.newBuilder().addValues(codec.decode(row.get(i))).build());
      }
    }
    return payloadBuilder.setValue(Any.pack(resultSetBuilder.build())).build();
  }

  private TypeSpec convertType(ColumnType columnType) throws StatusException {
    Builder builder = TypeSpec.newBuilder().setType(TypeSpec.Type.forNumber(columnType.id()));

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
          udtBuilder.putFields(column.name(), convertType(column.type()));
        }
        builder.setUdt(udtBuilder.build());
        break;
    }

    return builder.build();
  }
}
