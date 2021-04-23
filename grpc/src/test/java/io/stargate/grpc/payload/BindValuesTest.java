package io.stargate.grpc.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Result.Prepared;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.grpc.codec.cql.ValueCodecs;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import io.stargate.proto.QueryOuterClass.Values;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BindValuesTest {
  @ParameterizedTest
  @MethodSource({"validPayloads", "emptyPayload", "unsetAndNullPayloads"})
  public void bindValuesValid(
      Payload.Type type, Prepared prepared, Payload payload, PayloadValuesValidator validator)
      throws Exception {
    PayloadHandler handler = PayloadHandlers.HANDLERS.get(type);
    assertThat(handler).isNotNull();

    BoundStatement statement = handler.bindValues(prepared, payload, Utils.UNSET);

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
            CqlPayloadValidator.INSTANCE));
  }

  public static Stream<Arguments> emptyPayload() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(),
            makeCqlPayload(Values.newBuilder().build()),
            CqlPayloadValidator.INSTANCE));
  }

  public static Stream<Arguments> unsetAndNullPayloads() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .addValues(Value.newBuilder().setUnset(Unset.newBuilder().build()).build())
                    .build()),
            CqlPayloadValidator.INSTANCE),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValueNames("v1")
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .addValueNames("v2")
                    .addValues(Value.newBuilder().setUnset(Unset.newBuilder().build()).build())
                    .build()),
            CqlPayloadValidator.INSTANCE));
  }

  @ParameterizedTest
  @MethodSource({"invalidArityPayloads", "invalidNamesPayloads", "invalidTypePayloads"})
  public void bindValuesInvalid(
      Payload.Type type,
      Prepared prepared,
      Payload payload,
      Class<?> expectedException,
      String expectedMessage) {
    PayloadHandler handler = PayloadHandlers.HANDLERS.get(type);
    assertThat(handler).isNotNull();

    assertThatThrownBy(
            () -> {
              handler.bindValues(prepared, payload, Utils.UNSET);
            })
        .isInstanceOf(expectedException)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidArityPayloads() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .build()),
            StatusException.class,
            "Invalid number of bind values. Expected 0, but received 1"),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(Values.newBuilder().build()),
            StatusException.class,
            "Invalid number of bind values. Expected 2, but received 0"),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .build()),
            StatusException.class,
            "Invalid number of bind values. Expected 2, but received 3"));
  }

  public static Stream<Arguments> invalidNamesPayloads() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(),
            makeCqlPayload(Values.newBuilder().addValueNames("v1").build()),
            StatusException.class,
            "Invalid number of bind names. Expected 0, but received 1"),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValueNames("v1")
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .addValueNames("v2")
                    .build()),
            StatusException.class,
            "Invalid number of bind names. Expected 1, but received 2"),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValueNames("doesNotExist")
                    .addValues(Value.newBuilder().setNull(Null.newBuilder().build()).build())
                    .build()),
            StatusException.class,
            "Unable to find bind marker with name 'doesNotExist'"));
  }

  public static Stream<Arguments> invalidTypePayloads() {
    return Stream.of(
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setString("notAnInteger").build())
                    .build()),
            StatusException.class,
            "Invalid argument at position 1"),
        arguments(
            Payload.Type.TYPE_CQL,
            Utils.makePrepared(Column.create("v1", Type.Int), Column.create("v2", Type.Text)),
            makeCqlPayload(
                Values.newBuilder()
                    .addValues(Value.newBuilder().setInt(123).build())
                    .addValues(Value.newBuilder().setInt(456).build())
                    .build()),
            StatusException.class,
            "Invalid argument at position 2"));
  }

  private static Payload makeCqlPayload(Values values) {
    return Payload.newBuilder().setType(Payload.Type.TYPE_CQL).setValue(Any.pack(values)).build();
  }

  private interface PayloadValuesValidator {
    void validate(BoundStatement statement, Prepared prepared, Payload payload)
        throws InvalidProtocolBufferException;
  }

  private static class CqlPayloadValidator implements PayloadValuesValidator {
    public static final PayloadValuesValidator INSTANCE = new CqlPayloadValidator();

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
                        decodeValue(column.type().rawType(), statement.values().get(index));
                    assertThat(decodedValue).isEqualTo(values.getValues(index));
                  }
                });
      } else {
        List<Column> columns = prepared.metadata.columns;
        for (int i = 0; i < columns.size(); ++i) {
          Column column = columns.get(i);
          Value decodedValue = decodeValue(column.type().rawType(), statement.values().get(i));
          assertThat(decodedValue).isEqualTo(values.getValues(i));
        }
      }
    }

    private Value decodeValue(Column.Type type, ByteBuffer bytes) {
      if (bytes == null) {
        return Value.newBuilder().setNull(Null.newBuilder().build()).build();
      } else if (bytes == Utils.UNSET) {
        return Value.newBuilder().setUnset(Unset.newBuilder().build()).build();
      } else {
        return ValueCodecs.CODECS.get(type).decode(bytes);
      }
    }
  }
}
