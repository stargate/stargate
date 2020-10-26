package io.stargate.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedInputType;
import graphql.schema.GraphQLNamedOutputType;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchemaElement;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FieldTypeCachesTest {
  @Mock private NameMapping nameMapping;

  private FieldInputTypeCache fieldInputTypes;
  private FieldOutputTypeCache fieldOutputTypes;

  private List<String> warnings = new ArrayList<>();

  @BeforeEach
  public void setup() {
    fieldInputTypes = new FieldInputTypeCache(nameMapping, warnings);
    fieldOutputTypes = new FieldOutputTypeCache(nameMapping, warnings);
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportScalarTypes(
      Column.Type dbType, GraphQLScalarType gqlType) {

    assertThat(getInputType(dbType)).isEqualTo(gqlType);
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportList(Column.Type scalarDbType, GraphQLScalarType gqlType) {
    Column.ColumnType listType = Column.Type.List.of(scalarDbType);
    assertThat(getInputType(listType)).isEqualTo(new GraphQLList(gqlType));
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportSet(Column.Type scalarDbType, GraphQLScalarType gqlType) {
    Column.ColumnType listType = Column.Type.Set.of(scalarDbType);
    assertThat(getInputType(listType)).isEqualTo(new GraphQLList(gqlType));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldSupportMapInput(
      Column.ColumnType keyDbType, Column.ColumnType valueDbType, String name) {
    Column.ColumnType mapDbType = Column.Type.Map.of(keyDbType, valueDbType);
    GraphQLType graphTypeParentType = getInputType(mapDbType);
    assertThat(graphTypeParentType).isInstanceOf(GraphQLList.class);
    GraphQLSchemaElement graphType = graphTypeParentType.getChildren().get(0);
    assertThat(graphType).isInstanceOf(GraphQLInputType.class);
    GraphQLNamedInputType graphQLInputType = (GraphQLNamedInputType) graphType;
    assertThat(graphQLInputType.getName()).isEqualTo(name + "Input");
    assertThat(graphQLInputType.getChildren()).hasSize(2);
    List<GraphQLInputObjectField> fields =
        graphQLInputType.getChildren().stream()
            .map(f -> (GraphQLInputObjectField) f)
            .collect(Collectors.toList());

    assertThat(fields.get(0).getName()).isEqualTo("key");
    assertThat(fields.get(0).getType()).isEqualTo(GraphQLNonNull.nonNull(getInputType(keyDbType)));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    assertThat(fields.get(1).getType()).isEqualTo(getInputType(valueDbType));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldSupportMapOutput(
      Column.ColumnType keyDbType, Column.ColumnType valueDbType, String name) {
    Column.ColumnType mapDbType = Column.Type.Map.of(keyDbType, valueDbType);
    GraphQLType graphTypeParentType = getOutputType(mapDbType);
    assertThat(graphTypeParentType).isInstanceOf(GraphQLList.class);
    GraphQLSchemaElement graphType = graphTypeParentType.getChildren().get(0);
    assertThat(graphType).isInstanceOf(GraphQLOutputType.class);
    GraphQLNamedOutputType graphOutputType = (GraphQLNamedOutputType) graphType;
    assertThat(graphOutputType.getName()).isEqualTo(name);
    assertThat(graphOutputType.getChildren()).hasSize(2);
    List<GraphQLFieldDefinition> fields =
        graphOutputType.getChildren().stream()
            .map(f -> (GraphQLFieldDefinition) f)
            .collect(Collectors.toList());

    assertThat(fields.get(0).getName()).isEqualTo("key");
    assertThat(fields.get(0).getType()).isEqualTo(GraphQLNonNull.nonNull(getOutputType(keyDbType)));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    assertThat(fields.get(1).getType()).isEqualTo(getOutputType(valueDbType));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldReuseTheSameInstanceForMaps(
      Column.ColumnType keyDbType, Column.ColumnType valueDbType) {
    Column.ColumnType mapDbType = Column.Type.Map.of(keyDbType, valueDbType);
    GraphQLType graphTypeParentType = getOutputType(mapDbType);
    assertThat(graphTypeParentType).isInstanceOf(GraphQLList.class);
    GraphQLSchemaElement childObjectType = graphTypeParentType.getChildren().get(0);

    // Following calls should yield the same instance
    assertThat(childObjectType).isSameAs(getOutputType(mapDbType).getChildren().get(0));
  }

  @ParameterizedTest
  @MethodSource("getMapNestedArgs")
  public void getGraphQLTypeShouldSupportMapNestedInput(
      Column.ColumnType keyDbType, Column.ColumnType valueDbType, String nameInput) {

    Column.ColumnType mapDbType = Column.Type.Map.of(keyDbType, valueDbType);
    testNestedMaps(getInputType(mapDbType), nameInput);
  }

  @ParameterizedTest
  @MethodSource("getMapNestedArgs")
  public void getGraphQLTypeShouldSupportMapNestedOutput(
      Column.ColumnType keyDbType,
      Column.ColumnType valueDbType,
      @SuppressWarnings("unused") String nameInput,
      String nameOutput) {

    Column.ColumnType mapDbType = Column.Type.Map.of(keyDbType, valueDbType);
    testNestedMaps(getOutputType(mapDbType), nameOutput);
  }

  @ParameterizedTest
  @MethodSource("getTupleArgs")
  public void shouldSupportTupleAsOutputType(ColumnType[] subTypes, String name) {
    GraphQLType type = getOutputType(Type.Tuple.of(subTypes));
    assertThat(type).isInstanceOf(GraphQLObjectType.class);

    GraphQLObjectType objectType = (GraphQLObjectType) type;
    assertThat(objectType.getName()).matches(String.format("^Tuple%s$", name));

    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields)
        .hasSize(subTypes.length)
        .first()
        .matches(d -> d.getName().equals("item0"))
        .extracting(GraphQLFieldDefinition::getType)
        // The first item is not null
        .isInstanceOf(GraphQLNonNull.class)
        .extracting(t -> t.getChildren().get(0))
        .isSameAs(getOutputType(subTypes[0]));

    for (int i = 1; i < subTypes.length; i++) {
      GraphQLFieldDefinition field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      assertThat(field.getType()).isEqualTo(getOutputType(subTypes[i]));
    }
  }

  @ParameterizedTest
  @MethodSource("getTupleArgs")
  public void shouldSupportTupleAsInputType(ColumnType[] subTypes, String name) {
    GraphQLType type = getInputType(Type.Tuple.of(subTypes));
    assertThat(type).isInstanceOf(GraphQLInputObjectType.class);

    GraphQLInputObjectType objectType = (GraphQLInputObjectType) type;
    assertThat(objectType.getName()).matches(String.format("^Tuple%sInput$", name));

    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields)
        .hasSize(subTypes.length)
        .first()
        .matches(d -> d.getName().equals("item0"))
        .extracting(GraphQLInputObjectField::getType)
        // The first item is not null
        .isInstanceOf(GraphQLNonNull.class)
        .extracting(t -> t.getChildren().get(0))
        .isSameAs(getInputType(subTypes[0]));

    for (int i = 1; i < subTypes.length; i++) {
      GraphQLInputObjectField field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      assertThat(field.getType()).isEqualTo(getInputType(subTypes[i]));
    }
  }

  private static void testNestedMaps(GraphQLType parentGraphType, String name) {
    assertThat(parentGraphType).isInstanceOf(GraphQLList.class);
    GraphQLSchemaElement graphType = parentGraphType.getChildren().get(0);
    assertThat(graphType).isInstanceOf(GraphQLNamedType.class);
    GraphQLNamedType graphQLInputType = (GraphQLNamedType) graphType;
    assertThat(graphQLInputType.getName()).isEqualTo(name);
    assertThat(graphQLInputType.getChildren()).hasSize(2);
    List<GraphQLNamedSchemaElement> fields =
        graphQLInputType.getChildren().stream()
            .map(f -> (GraphQLNamedSchemaElement) f)
            .collect(Collectors.toList());

    assertThat(fields.get(0).getName()).isEqualTo("key");
    assertThat(fields.get(1).getName()).isEqualTo("value");
  }

  public static Stream<Arguments> getScalarTypes() {
    return Stream.of(
        arguments(Column.Type.Ascii, CustomScalar.ASCII.getGraphQLScalar()),
        arguments(Column.Type.Bigint, CustomScalar.BIGINT.getGraphQLScalar()),
        arguments(Column.Type.Blob, CustomScalar.BLOB.getGraphQLScalar()),
        arguments(Column.Type.Boolean, Scalars.GraphQLBoolean),
        arguments(Column.Type.Counter, CustomScalar.COUNTER.getGraphQLScalar()),
        arguments(Column.Type.Decimal, CustomScalar.DECIMAL.getGraphQLScalar()),
        arguments(Column.Type.Double, Scalars.GraphQLFloat),
        arguments(Column.Type.Int, Scalars.GraphQLInt),
        arguments(Column.Type.Text, Scalars.GraphQLString),
        arguments(Column.Type.Varchar, Scalars.GraphQLString),
        arguments(Column.Type.Timestamp, CustomScalar.TIMESTAMP.getGraphQLScalar()),
        arguments(Column.Type.Uuid, CustomScalar.UUID.getGraphQLScalar()),
        arguments(Column.Type.Varint, CustomScalar.VARINT.getGraphQLScalar()),
        arguments(Column.Type.Timeuuid, CustomScalar.TIMEUUID.getGraphQLScalar()),
        arguments(Column.Type.Inet, CustomScalar.INET.getGraphQLScalar()),
        arguments(Column.Type.Date, CustomScalar.DATE.getGraphQLScalar()),
        arguments(Column.Type.Time, CustomScalar.TIME.getGraphQLScalar()),
        arguments(Column.Type.Smallint, CustomScalar.SMALLINT.getGraphQLScalar()),
        arguments(Column.Type.Tinyint, CustomScalar.TINYINT.getGraphQLScalar()));
  }

  public static Stream<Arguments> getMapArgs() {
    return Stream.of(
        arguments(Column.Type.Text, Column.Type.Uuid, "EntryStringKeyUuidValue"),
        arguments(Column.Type.Int, Column.Type.Timeuuid, "EntryIntKeyTimeUuidValue"),
        arguments(Column.Type.Timeuuid, Column.Type.Bigint, "EntryTimeUuidKeyBigIntValue"),
        arguments(Column.Type.Timestamp, Column.Type.Inet, "EntryTimestampKeyInetValue"),
        arguments(
            Column.Type.Uuid,
            Column.Type.List.of(Column.Type.Float),
            "EntryUuidKeyListFloat32Value"),
        arguments(
            Column.Type.Smallint,
            Column.Type.Set.of(Column.Type.Double),
            "EntrySmallIntKeyListFloatValue"));
  }

  public static Stream<Arguments> getMapNestedArgs() {
    return Stream.of(
        arguments(
            Column.Type.Tinyint,
            Column.Type.Map.of(Column.Type.Uuid, Column.Type.Int),
            "EntryTinyIntKeyListEntryUuidKeyIntValueInputValueInput",
            "EntryTinyIntKeyListEntryUuidKeyIntValueValue"),
        arguments(
            Column.Type.Map.of(Column.Type.Text, Column.Type.Bigint),
            Column.Type.Set.of(Column.Type.Double),
            "EntryListEntryStringKeyBigIntValueInputKeyListFloatValueInput",
            "EntryListEntryStringKeyBigIntValueKeyListFloatValue"));
  }

  public static Stream<Arguments> getTupleArgs() {
    return Stream.of(
        arguments(new ColumnType[] {Type.Text, Type.Uuid}, "StringUuid"),
        arguments(new ColumnType[] {Type.Uuid, Type.Set.of(Column.Type.Double)}, "UuidListFloat"),
        arguments(
            new ColumnType[] {Type.Uuid, Type.Map.of(Type.Timeuuid, Column.Type.Double)},
            "UuidListEntryTimeUuidKeyFloatValue(Input)?"),
        arguments(new ColumnType[] {Type.Int, Type.Timeuuid}, "IntTimeUuid"),
        arguments(new ColumnType[] {Type.Double}, "Float"),
        arguments(
            new ColumnType[] {Type.Uuid, Type.Decimal, Type.Timestamp}, "UuidDecimalTimestamp"),
        arguments(new ColumnType[] {Type.Float, Type.Float}, "Float32Float32"));
  }

  /** Gets a GraphQL input type using the shared cache */
  private GraphQLInputType getInputType(Column.ColumnType dbType) {
    GraphQLInputType type = fieldInputTypes.get(dbType);
    assertThat(type).isInstanceOf(GraphQLInputType.class);
    return type;
  }

  /** Gets a GraphQL output type using the shared cache */
  private GraphQLOutputType getOutputType(Column.ColumnType dbType) {
    GraphQLOutputType type = fieldOutputTypes.get(dbType);
    assertThat(type).isInstanceOf(GraphQLOutputType.class);
    return type;
  }
}
