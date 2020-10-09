package io.stargate.producer.kafka.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.stream.Stream;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Collection.Kind;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.CQLType.Tuple;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CqlToAvroTypeConverterTest {

  @ParameterizedTest
  @MethodSource("tuplesProvider")
  public void shouldConstructTupleName(Tuple tuple, String expected) {
    assertThat(CqlToAvroTypeConverter.tupleToRecordName(tuple)).isEqualTo(expected);
  }

  public static Stream<Arguments> tuplesProvider() {
    return Stream.of(
        Arguments.of(new Tuple(Native.INT), "tuple_int_"),
        Arguments.of(new Tuple(Native.INT, Native.TEXT), "tuple_int_text_"),
        Arguments.of(
            new Tuple(Native.INT, Native.TEXT, new Collection(Kind.LIST, Native.INT)),
            "tuple_int_text_list_int__"),
        Arguments.of(
            new Tuple(new Tuple(Native.INT, new Collection(Kind.SET, Native.ASCII)), Native.TEXT),
            "tuple_tuple_int_set_ascii___text_"));
  }
}
