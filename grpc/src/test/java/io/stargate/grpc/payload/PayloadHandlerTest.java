package io.stargate.grpc.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.grpc.codec.cql.ValueCodecs;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PayloadHandlerTest {
  private static ByteBuffer UNSET = ByteBuffer.allocate(1);

  @ParameterizedTest
  @MethodSource({"validPayloads"})
  public void bindValues(
      Payload.Type type, Prepared prepared, Payload payload, PayloadValidator validator)
      throws Exception {
    PayloadHandler handler = PayloadHandlers.HANDLERS.get(type);
    assertThat(handler).isNotNull();

    BoundStatement statement = handler.bindValues(prepared, payload, UNSET);

    validator.validate(statement, prepared, payload);
  }

  public static Stream<Arguments> validPayloads() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(),
            makeCqlPayload(Values.newBuilder().build()),
            CqlPayloadValidator.INSTANCE),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setInt(123).build())
                    .addValues(Value.newBuilder().setString("abc").build())
                    .build()),
            CqlPayloadValidator.INSTANCE),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValueNames("v1")
                    .addValues(Value.newBuilder().setInt(123).build())
                    .addValueNames("v2")
                    .addValues(Value.newBuilder().setString("abc").build())
                    .build()),
            CqlPayloadValidator.INSTANCE)
        // TODO: UNSET, null, UUID
        );
  }

  private static Payload makeCqlPayload(Values values) {
    return Payload.newBuilder().setType(Payload.Type.TYPE_CQL).setValue(Any.pack(values)).build();
  }

  private interface PayloadValidator {
    void validate(BoundStatement statement, Prepared prepared, Payload payload)
        throws InvalidProtocolBufferException;
  }

  private static class CqlPayloadValidator implements PayloadValidator {
    public static final PayloadValidator INSTANCE = new CqlPayloadValidator();

    private CqlPayloadValidator() {}

    @Override
    public void validate(BoundStatement statement, Prepared prepared, Payload payload)
        throws InvalidProtocolBufferException {
      assertThat(payload.getType()).isEqualTo(Payload.Type.TYPE_CQL);

      Values values = payload.getValue().unpack(Values.class);
      assertThat(values.getValuesCount()).isEqualTo(statement.values().size());

      if (values.getValueNamesCount() > 0) {
        assertThat(statement.boundNames()).isPresent();
        statement
            .boundNames()
            .ifPresent(
                names -> {
                  assertThat(names).isEqualTo(new ArrayList<>(values.getValueNamesList()));
                  List<Column> columns = prepared.metadata.columns;
                  for (String name : names) {
                    int index =
                        IntStream.of(0, columns.size() - 1)
                            .filter(i -> columns.get(i).name().equals(name))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Unable to find column"));
                    Column column = columns.get(index);
                    Value decodedValue =
                        ValueCodecs.CODECS
                            .get(column.type().rawType())
                            .decode(statement.values().get(index));
                    assertThat(decodedValue).isEqualTo(values.getValues(index));
                  }
                });
      } else {
        List<Column> columns = prepared.metadata.columns;
        for (int i = 0; i < columns.size(); ++i) {
          Column column = columns.get(i);
          Value decodedValue =
              ValueCodecs.CODECS.get(column.type().rawType()).decode(statement.values().get(i));
          assertThat(decodedValue).isEqualTo(values.getValues(i));
        }
      }
    }
  }
}
