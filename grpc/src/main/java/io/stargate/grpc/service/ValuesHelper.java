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
package io.stargate.grpc.service;

import static io.stargate.grpc.codec.ValueCodec.decodeValue;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
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
import io.stargate.grpc.codec.ValueCodec;
import io.stargate.grpc.codec.ValueCodecs;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ValuesHelper {
  public static BoundStatement bindValues(Prepared prepared, Values values, ByteBuffer unsetValue)
      throws StatusException {
    final List<Column> columns = prepared.metadata.columns;
    final int columnCount = columns.size();
    final int valuesCount = values.getValuesCount();
    if (columnCount != valuesCount) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Invalid number of bind values. Expected %d, but received %d",
                  columnCount, valuesCount))
          .asException();
    }
    final List<ByteBuffer> boundValues = new ArrayList<>(columnCount);
    List<String> boundValueNames = null;
    if (values.getValueNamesCount() != 0) {
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
        ValueCodec codec = ValueCodecs.get(columnType.rawType());
        Value value = values.getValues(i);
        try {
          boundValues.add(encodeValue(codec, value, columnType, unsetValue));
        } catch (Exception e) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format("Invalid argument for name '%s': %s", name, e.getMessage()))
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
        ValueCodec codec = ValueCodecs.get(columnType.rawType());
        try {
          boundValues.add(encodeValue(codec, value, columnType, unsetValue));
        } catch (Exception e) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format("Invalid argument at position %d: %s", i + 1, e.getMessage()))
              .withCause(e)
              .asException();
        }
      }
    }

    return new BoundStatement(prepared.statementId, boundValues, boundValueNames);
  }

  public static ResultSet processResult(Rows rows, QueryParameters parameters)
      throws StatusException {
    return processResult(rows, parameters.getSkipMetadata(), null, null, null);
  }

  public static ResultSet processResult(Rows rows, BatchParameters parameters)
      throws StatusException {
    return processResult(rows, parameters.getSkipMetadata(), null, null, null);
  }

  public static ResultSet processResult(
      Rows rows,
      QueryParameters parameters,
      Function<io.stargate.db.datastore.Row, ByteBuffer> getComparableBytes,
      Function<io.stargate.db.datastore.Row, ByteBuffer> getPagingState,
      BiFunction<List<Column>, List<ByteBuffer>, io.stargate.db.datastore.Row> makeRow)
      throws StatusException {
    return processResult(
        rows, parameters.getSkipMetadata(), getComparableBytes, getPagingState, makeRow);
  }

  private static ResultSet processResult(
      Rows rows,
      boolean skipMetadata,
      Function<io.stargate.db.datastore.Row, ByteBuffer> getComparableBytes,
      Function<io.stargate.db.datastore.Row, ByteBuffer> getPagingState,
      BiFunction<List<Column>, List<ByteBuffer>, io.stargate.db.datastore.Row> makeRow)
      throws StatusException {
    final List<Column> columns = rows.resultMetadata.columns;
    final int columnCount = columns.size();

    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();
    if (!skipMetadata) {
      for (Column column : columns) {
        resultSetBuilder.addColumns(
            ColumnSpec.newBuilder()
                .setType(convertType(columnTypeNotNull(column)))
                .setName(column.name())
                .build());
      }
    }

    for (List<ByteBuffer> row : rows.rows) {
      ByteBuffer comparableBytes = null;
      ByteBuffer rowPagingState = null;
      io.stargate.db.datastore.Row arrayListRow = makeRow.apply(columns, row);
      if (arrayListRow != null) {
        comparableBytes = getComparableBytes.apply(arrayListRow);
        rowPagingState = getPagingState.apply(arrayListRow);
      }
      Row.Builder rowBuilder = Row.newBuilder();
      for (int i = 0; i < columnCount; ++i) {
        ColumnType columnType = columnTypeNotNull(columns.get(i));
        ValueCodec codec = ValueCodecs.get(columnType.rawType());
        rowBuilder.addValues(decodeValue(codec, row.get(i), columnType));
      }
      if (comparableBytes != null) {
        rowBuilder.setComparableBytes(
            BytesValue.newBuilder().setValue(ByteString.copyFrom(comparableBytes)).build());
      }
      if (rowPagingState != null) {
        rowBuilder.setPagingState(
            BytesValue.newBuilder().setValue(ByteString.copyFrom(rowPagingState)).build());
      }
      resultSetBuilder.addRows(rowBuilder);
    }

    if (rows.resultMetadata.pagingState != null) {
      resultSetBuilder.setPagingState(
          BytesValue.newBuilder()
              .setValue(ByteString.copyFrom(rows.resultMetadata.pagingState))
              .build());
    }

    return resultSetBuilder.build();
  }

  @Nullable
  public static ByteBuffer encodeValue(
      ValueCodec codec, Value value, ColumnType columnType, ByteBuffer unsetValue) {
    if (value.hasUnset()) {
      return unsetValue;
    } else {
      return ValueCodec.encodeValue(codec, value, columnType);
    }
  }

  @NonNull
  public static ColumnType columnTypeNotNull(Column column) throws StatusException {
    ColumnType type = column.type();
    if (type == null) {
      throw Status.INTERNAL
          .withDescription(String.format("Column '%s' doesn't have a valid type", column.name()))
          .asException();
    }
    return type;
  }

  public static TypeSpec convertType(ColumnType columnType) throws StatusException {
    TypeSpec.Builder builder = TypeSpec.newBuilder();
    List<ColumnType> parameters;

    switch (columnType.rawType()) {
      case List:
        parameters = columnType.parameters();
        if (parameters.size() != 1) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Expected list type to have a parameterized type")
              .asException();
        }
        builder.setList(
            TypeSpec.List.newBuilder()
                .setFrozen(columnType.isFrozen())
                .setElement(convertType(parameters.get(0)))
                .build());
        break;
      case Map:
        parameters = columnType.parameters();
        if (parameters.size() != 2) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Expected map type to have key/value parameterized types")
              .asException();
        }
        builder.setMap(
            TypeSpec.Map.newBuilder()
                .setFrozen(columnType.isFrozen())
                .setKey(convertType(parameters.get(0)))
                .setValue(convertType(parameters.get(1)))
                .build());
        break;
      case Set:
        parameters = columnType.parameters();
        if (parameters.size() != 1) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Expected set type to have a parameterized type")
              .asException();
        }
        builder.setSet(
            TypeSpec.Set.newBuilder()
                .setFrozen(columnType.isFrozen())
                .setElement(convertType(parameters.get(0)))
                .build());
        break;
      case Tuple:
        parameters = columnType.parameters();
        if (parameters.isEmpty()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Expected tuple type to have at least one parameterized type")
              .asException();
        }
        TypeSpec.Tuple.Builder tupleBuilder = TypeSpec.Tuple.newBuilder();
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
        TypeSpec.Udt.Builder udtBuilder = TypeSpec.Udt.newBuilder();
        udtBuilder.setName(udt.name());
        for (Column column : udt.columns()) {
          udtBuilder.putFields(column.name(), convertType(columnTypeNotNull(column)));
        }
        udtBuilder.setFrozen(columnType.isFrozen());
        builder.setUdt(udtBuilder.build());
        break;
      default:
        builder.setBasic(
            Objects.requireNonNull(
                TypeSpec.Basic.forNumber(columnType.id()), "Unhandled parameterized type"));
    }

    return builder.build();
  }
}
