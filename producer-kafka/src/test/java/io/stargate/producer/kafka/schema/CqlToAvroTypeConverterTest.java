package io.stargate.producer.kafka.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.LinkedHashMap;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Collection.Kind;
import org.apache.cassandra.stargate.schema.CQLType.Custom;
import org.apache.cassandra.stargate.schema.CQLType.MapDataType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.CQLType.Tuple;
import org.apache.cassandra.stargate.schema.CQLType.UserDefined;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CqlToAvroTypeConverterTest {

  @ParameterizedTest
  @MethodSource("tuplesProvider")
  public void shouldCreateTupleName(Tuple tuple, String expected) {
    assertThat(CqlToAvroTypeConverter.tupleToRecordName(tuple)).isEqualTo(expected);
  }

  @Test
  public void shouldCreateSchemaForSimpleTuple() {
    // when
    Schema schema = CqlToAvroTypeConverter.creteTupleSchema(new Tuple(Native.INT));

    // then
    assertThat(schema.getField("t_0").schema().getType()).isEqualTo(Type.INT);
  }

  @Test
  public void shouldCreateSchemaForComplexTuple() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.creteTupleSchema(
            new Tuple(Native.INT, Native.TEXT, new Collection(Kind.LIST, Native.INT)));

    // then
    assertThat(schema.getField("t_0").schema().getType()).isEqualTo(Type.INT);
    assertThat(schema.getField("t_1").schema().getType()).isEqualTo(Type.STRING);
    assertThat(schema.getField("t_2").schema().getType()).isEqualTo(Type.ARRAY);
  }

  @Test
  public void shouldCreateSchemaForNestedTuple() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.creteTupleSchema(
            new Tuple(new Tuple(Native.INT, new Collection(Kind.SET, Native.ASCII)), Native.TEXT));

    // then
    Schema nested = schema.getField("t_0").schema();
    assertThat(nested.getType()).isEqualTo(Type.RECORD);
    assertThat(nested.getField("t_0").schema().getType()).isEqualTo(Type.INT);
    assertThat(nested.getField("t_1").schema().getType()).isEqualTo(Type.ARRAY);
    assertThat(schema.getField("t_1").schema().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForCustomType() {
    // when
    Schema schema = CqlToAvroTypeConverter.createCustomSchema(new Custom("class"));

    // then
    assertThat(schema.getType()).isEqualTo(Type.BYTES);
  }

  @Test
  public void shouldCreateSimpleUserDefinedSchema() {
    // when
    LinkedHashMap<String, CQLType> udtColumns = new LinkedHashMap<>();
    udtColumns.put("udtcol_1", Native.INT);
    udtColumns.put("udtcol_2", Native.TEXT);
    UserDefined userDefinedType = new UserDefined("ks", "typeName", udtColumns);
    Schema schema = CqlToAvroTypeConverter.createUserDefinedSchema(userDefinedType);

    // then
    assertThat(schema.getType()).isEqualTo(Type.RECORD);
    assertThat(schema.getField("udtcol_1").schema().getType()).isEqualTo(Type.INT);
    assertThat(schema.getField("udtcol_2").schema().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateNestedUserDefinedSchema() {
    // when
    LinkedHashMap<String, CQLType> udtColumns = new LinkedHashMap<>();
    udtColumns.put("udtcol_1", Native.INT);
    udtColumns.put("udtcol_2", Native.TEXT);
    UserDefined userDefinedType = new UserDefined("ks", "typeName", udtColumns);
    LinkedHashMap<String, CQLType> nestedUdtColumns = new LinkedHashMap<>();
    nestedUdtColumns.put("nested", userDefinedType);
    nestedUdtColumns.put("list", new Collection(Kind.LIST, Native.INT));
    UserDefined userDefinedTypeNested = new UserDefined("ks", "nested", nestedUdtColumns);
    Schema schema = CqlToAvroTypeConverter.createUserDefinedSchema(userDefinedTypeNested);

    // then
    Schema nested = schema.getField("nested").schema();
    assertThat(schema.getType()).isEqualTo(Type.RECORD);
    assertThat(schema.getField("list").schema().getType()).isEqualTo(Type.ARRAY);
    assertThat(nested.getType()).isEqualTo(Type.RECORD);
    assertThat(nested.getField("udtcol_1").schema().getType()).isEqualTo(Type.INT);
    assertThat(nested.getField("udtcol_2").schema().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForList() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createCollectionSchema(new Collection(Kind.LIST, Native.TEXT));

    // then
    assertThat(schema.getType()).isEqualTo(Type.ARRAY);
    assertThat(schema.getElementType().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForSet() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createCollectionSchema(new Collection(Kind.SET, Native.TEXT));

    // then
    assertThat(schema.getType()).isEqualTo(Type.ARRAY);
    assertThat(schema.getElementType().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForMap() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createMapSchema(new MapDataType(Native.INT, Native.TEXT));

    // then
    assertThat(schema.getType()).isEqualTo(Type.MAP);
    assertThat(schema.getValueType().getType()).isEqualTo(Type.STRING);
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
