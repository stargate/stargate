package io.stargate.producer.kafka.schema;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableListType;
import io.stargate.db.schema.ImmutableMapType;
import io.stargate.db.schema.ImmutableSetType;
import io.stargate.db.schema.ImmutableTupleType;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CqlToAvroTypeConverterTest {

  @ParameterizedTest
  @MethodSource("tuplesProvider")
  public void shouldCreateTupleName(ParameterizedType.TupleType tuple, String expected) {
    assertThat(CqlToAvroTypeConverter.tupleToRecordName(tuple)).isEqualTo(expected);
  }

  @Test
  public void shouldCreateSchemaForSimpleTuple() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createTupleSchema(
            ImmutableTupleType.builder().addParameters(Column.Type.Int).build());

    // then
    assertThat(schema.getField("t_0").schema().getType()).isEqualTo(Type.INT);
  }

  @Test
  public void shouldCreateSchemaForComplexTuple() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createTupleSchema(
            ImmutableTupleType.builder()
                .addParameters(
                    Column.Type.Int,
                    Column.Type.Text,
                    ImmutableListType.builder().addParameters(Column.Type.Int).build())
                .build());

    // then
    assertThat(schema.getField("t_0").schema().getType()).isEqualTo(Type.INT);
    assertThat(schema.getField("t_1").schema().getType()).isEqualTo(Type.STRING);
    assertThat(schema.getField("t_2").schema().getType()).isEqualTo(Type.ARRAY);
  }

  @Test
  public void shouldCreateSchemaForNestedTuple() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createTupleSchema(
            ImmutableTupleType.builder()
                .addParameters(
                    ImmutableTupleType.builder()
                        .addParameters(
                            Column.Type.Int,
                            ImmutableSetType.builder().addParameters(Column.Type.Ascii).build())
                        .build(),
                    Column.Type.Text)
                .build());

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
    Schema schema = CqlToAvroTypeConverter.createCustomSchema();

    // then
    assertThat(schema.getType()).isEqualTo(Type.BYTES);
  }

  @Test
  public void shouldCreateSimpleUserDefinedSchema() {
    // when
    Column[] udtColumns = {
      ImmutableColumn.builder().name("udtcol_1").type(Column.Type.Int).build(),
      ImmutableColumn.builder().name("udtcol_2").type(Column.Type.Text).build()
    };
    UserDefinedType userDefinedType =
        ImmutableUserDefinedType.builder()
            .keyspace("ks")
            .name("typeName")
            .addColumns(udtColumns)
            .build();
    Schema schema = CqlToAvroTypeConverter.createUserDefinedSchema(userDefinedType);

    // then
    assertThat(schema.getType()).isEqualTo(Type.RECORD);
    assertThat(schema.getField("udtcol_1").schema().getType()).isEqualTo(Type.INT);
    assertThat(schema.getField("udtcol_2").schema().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateNestedUserDefinedSchema() {
    // when
    Column[] udtColumns = {
      ImmutableColumn.builder().name("udtcol_1").type(Column.Type.Int).build(),
      ImmutableColumn.builder().name("udtcol_2").type(Column.Type.Text).build()
    };

    UserDefinedType userDefinedType =
        ImmutableUserDefinedType.builder()
            .keyspace("ks")
            .name("typeName")
            .addColumns(udtColumns)
            .build();

    Column[] nestedUdtColumns = {
      ImmutableColumn.builder().name("nested").type(userDefinedType).build(),
      ImmutableColumn.builder()
          .name("list")
          .type(ImmutableListType.builder().addParameters(Column.Type.Int).build())
          .build()
    };
    UserDefinedType userDefinedTypeNested =
        ImmutableUserDefinedType.builder()
            .keyspace("ks")
            .name("nested")
            .addColumns(nestedUdtColumns)
            .build();
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
        CqlToAvroTypeConverter.createCollectionSchema(
            ImmutableListType.builder().addParameters(Column.Type.Text).build());

    // then
    assertThat(schema.getType()).isEqualTo(Type.ARRAY);
    assertThat(schema.getElementType().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForSet() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createCollectionSchema(
            ImmutableSetType.builder().addParameters(Column.Type.Text).build());

    // then
    assertThat(schema.getType()).isEqualTo(Type.ARRAY);
    assertThat(schema.getElementType().getType()).isEqualTo(Type.STRING);
  }

  @Test
  public void shouldCreateSchemaForMap() {
    // when
    Schema schema =
        CqlToAvroTypeConverter.createMapSchema(
            ImmutableMapType.builder().addParameters(Column.Type.Int, Column.Type.Text).build());

    // then
    assertThat(schema.getType()).isEqualTo(Type.MAP);
    assertThat(schema.getValueType().getType()).isEqualTo(Type.STRING);
  }

  public static Stream<Arguments> tuplesProvider() {
    return Stream.of(
        Arguments.of(
            ImmutableTupleType.builder().addParameters(Column.Type.Int).build(), "tuple_int_"),
        Arguments.of(
            ImmutableTupleType.builder().addParameters(Column.Type.Int, Column.Type.Text).build(),
            "tuple_int_text_"),
        Arguments.of(
            ImmutableTupleType.builder()
                .addParameters(
                    Column.Type.Int,
                    Column.Type.Text,
                    ImmutableListType.builder().addParameters(Column.Type.Int).build())
                .build(),
            "tuple_int_text_list_int__"),
        Arguments.of(
            ImmutableTupleType.builder()
                .addParameters(
                    ImmutableTupleType.builder()
                        .addParameters(
                            Column.Type.Int,
                            ImmutableSetType.builder().addParameters(Column.Type.Ascii).build())
                        .build(),
                    Column.Type.Text)
                .build(),
            "tuple_frozen_tuple_int_set_ascii____text_"));
  }
}
