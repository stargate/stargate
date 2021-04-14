package io.stargate.grpc.payload.cql;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.db.schema.Column;
import io.stargate.grpc.codec.cql.ValueCodec;
import io.stargate.grpc.codec.cql.ValueCodecs;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ValuesHandler implements PayloadHandler {
  @Override
  public BoundStatement bindValues(Prepared prepared, Payload payload)
      throws InvalidProtocolBufferException {
    final Values values = payload.getValue().unpack(Values.class);
    final List<Column> columns = prepared.metadata.columns;
    final int columnCount = columns.size();
    if (values == null && columnCount > 0 || columnCount != values.getValuesCount()) {
      throw new IllegalArgumentException("Invalid number of bind parameters");
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
          throw new IllegalArgumentException("Unsupported type");
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
          throw new IllegalArgumentException("Unsupported type");
        }
        boundValues.add(codec.encode(value, column.type()));
      }
    }

    return new BoundStatement(prepared.statementId, boundValues, boundValueNames);
  }

  @Override
  public Payload processResult(Rows rows) {
    Payload.Builder payloadBuilder = Payload.newBuilder().setType(Type.TYPE_CQL);

    final List<Column> columns = rows.resultMetadata.columns;
    final int columnCount = columns.size();

    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    // TODO: Add column types

    for (List<ByteBuffer> row : rows.rows) {
      for (int i = 0; i < columnCount; ++i) {
        Column column = columns.get(i);
        ValueCodec codec = ValueCodecs.CODECS.get(column.type());
        if (codec == null) {
          throw new IllegalArgumentException("Unsupported type");
        }
        resultSetBuilder.addRows(Row.newBuilder().addValues(codec.decode(row.get(i))).build());
      }
    }
    return payloadBuilder.setValue(Any.pack(resultSetBuilder.build())).build();
  }
}
