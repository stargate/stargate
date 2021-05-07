package io.stargate.it.grpc;

import static io.stargate.grpc.Values.collection;
import static io.stargate.grpc.Values.nullValue;
import static io.stargate.grpc.Values.uuidValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.grpc.StatusRuntimeException;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.proto.QueryOuterClass.Result;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE IF NOT EXISTS list_test (k text, v list<text>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS set_test (k text, v set<text>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS map_test (k text, v map<text, int>, PRIMARY KEY(k))",
      "CREATE TABLE IF NOT EXISTS tuple_test (k text, v tuple<text, int, uuid>, PRIMARY KEY(k))",
    })
public class CollectionsTest extends GrpcIntegrationTest {

  @ParameterizedTest
  @MethodSource("collectionValues")
  public void collections(Value collection, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    StringValue keyspaceValue = StringValue.of(keyspace.toString());

    Result result =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                cqlQueryParameters(stringValue("a"), collection).setKeyspace(keyspaceValue)));
    assertThat(result).isNotNull();

    result =
        stub.executeQuery(
            cqlQuery(
                String.format("SELECT * FROM %s WHERE k = ?", tableName),
                cqlQueryParameters(stringValue("a")).setKeyspace(keyspaceValue)));
    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(2);
    assertThat(rs.getRows(0).getValues(0)).isEqualTo(stringValue("a"));
    assertThat(rs.getRows(0).getValues(1)).isEqualTo(collection);
  }

  public static Stream<Arguments> collectionValues() {
    return Stream.of(
        Arguments.of(collection(stringValue("a"), stringValue("b"), stringValue("c")), "list_test"),
        Arguments.of(collection(stringValue("a"), stringValue("b"), stringValue("c")), "set_test"),
        Arguments.of(
            collection(
                stringValue("a"), intValue(1),
                stringValue("b"), intValue(2),
                stringValue("c"), intValue(3)),
            "map_test"),
        Arguments.of(collection(stringValue("a")), "tuple_test"),
        Arguments.of(collection(stringValue("a"), intValue(1)), "tuple_test"),
        Arguments.of(
            collection(stringValue("a"), intValue(1), uuidValue(UUID.randomUUID())), "tuple_test"),
        Arguments.of(
            collection(nullValue(), intValue(1), uuidValue(UUID.randomUUID())), "tuple_test"),
        Arguments.of(
            collection(stringValue("a"), nullValue(), uuidValue(UUID.randomUUID())), "tuple_test"),
        Arguments.of(collection(stringValue("a"), intValue(1), nullValue()), "tuple_test"),
        Arguments.of(collection(nullValue(), nullValue(), nullValue()), "tuple_test"));
  }

  @ParameterizedTest
  @MethodSource("emptyValues")
  public void emptyCollections(
      Value collection, Value expected, String tableName, @TestKeyspace CqlIdentifier keyspace)
      throws InvalidProtocolBufferException {
    StargateBlockingStub stub = stubWithCallCredentials();

    StringValue keyspaceValue = StringValue.of(keyspace.toString());

    Result result =
        stub.executeQuery(
            cqlQuery(
                String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                cqlQueryParameters(stringValue("b"), collection).setKeyspace(keyspaceValue)));
    assertThat(result).isNotNull();

    result =
        stub.executeQuery(
            cqlQuery(
                String.format("SELECT * FROM %s WHERE k = ?", tableName),
                cqlQueryParameters(stringValue("b")).setKeyspace(keyspaceValue)));
    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(2);
    assertThat(rs.getRows(0).getValues(0)).isEqualTo(stringValue("b"));
    assertThat(rs.getRows(0).getValues(1)).isEqualTo(expected);
  }

  public static Stream<Arguments> emptyValues() {
    return Stream.of(
        Arguments.of(collection(), nullValue(), "list_test"),
        Arguments.of(nullValue(), nullValue(), "list_test"),
        Arguments.of(collection(), nullValue(), "set_test"),
        Arguments.of(nullValue(), nullValue(), "set_test"),
        Arguments.of(collection(), nullValue(), "map_test"),
        Arguments.of(nullValue(), nullValue(), "map_test"),
        Arguments.of(collection(), collection(), "tuple_test"),
        Arguments.of(nullValue(), nullValue(), "tuple_test"));
  }

  @ParameterizedTest
  @MethodSource("invalidValues")
  public void invalidCollections(
      Value collection,
      String tableName,
      String expectedMessage,
      @TestKeyspace CqlIdentifier keyspace) {
    StargateBlockingStub stub = stubWithCallCredentials();

    StringValue keyspaceValue = StringValue.of(keyspace.toString());

    assertThatThrownBy(
            () -> {
              Result result =
                  stub.executeQuery(
                      cqlQuery(
                          String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName),
                          cqlQueryParameters(stringValue("b"), collection)
                              .setKeyspace(keyspaceValue)));
              assertThat(result).isNotNull();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        Arguments.of(
            collection(intValue(1)),
            "list_test",
            "Invalid argument at position 2: Expected string type"),
        Arguments.of(
            collection(nullValue()),
            "list_test",
            "Invalid argument at position 2: null is not supported inside lists"),
        Arguments.of(
            collection(intValue(1)),
            "set_test",
            "Invalid argument at position 2: Expected string type"),
        Arguments.of(
            collection(nullValue()),
            "set_test",
            "Invalid argument at position 2: null is not supported inside sets"),
        Arguments.of(
            collection(stringValue("a"), stringValue("b")),
            "map_test",
            "Invalid argument at position 2: Expected integer type"),
        Arguments.of(
            collection(stringValue("a"), intValue(1), stringValue("b")),
            "map_test",
            "Invalid argument at position 2: Missing pair value (expected an even number of elements)"),
        Arguments.of(
            collection(stringValue("a"), nullValue()),
            "map_test",
            "Invalid argument at position 2: null is not supported inside maps"),
        Arguments.of(
            collection(nullValue(), nullValue()),
            "map_test",
            "Invalid argument at position 2: null is not supported inside maps"),
        Arguments.of(
            collection(stringValue("a"), intValue(1), intValue(2)),
            "tuple_test",
            "Invalid argument at position 2: Expected UUID type"),
        Arguments.of(
            collection(
                stringValue("a"), intValue(1), uuidValue(UUID.randomUUID()), stringValue("b")),
            "tuple_test",
            "Invalid argument at position 2: Too many tuple fields. Expected 3, but received 4"));
  }
}
