package io.stargate.graphql.schema.cqlfirst.dml;

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
import io.stargate.graphql.schema.SchemaAssertions;
import io.stargate.graphql.schema.scalars.CqlScalar;
import java.util.ArrayList;
import java.util.Arrays;
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

  private final List<String> warnings = new ArrayList<>();

  @BeforeEach
  public void setup() {
    fieldInputTypes = new FieldInputTypeCache(nameMapping, warnings);
    fieldOutputTypes = new FieldOutputTypeCache(nameMapping, warnings);
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportScalarTypes(
      Column.Type dbType, GraphQLScalarType gqlType) {

    SchemaAssertions.assertSameTypes(getInputType(dbType), gqlType);
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportList(Column.Type scalarDbType, GraphQLScalarType gqlType) {
    Column.ColumnType listType = Column.Type.List.of(scalarDbType);
    SchemaAssertions.assertSameTypes(getInputType(listType), new GraphQLList(gqlType));
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportSet(Column.Type scalarDbType, GraphQLScalarType gqlType) {
    Column.ColumnType listType = Column.Type.Set.of(scalarDbType);
    SchemaAssertions.assertSameTypes(getInputType(listType), new GraphQLList(gqlType));
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
    SchemaAssertions.assertSameTypes(
        fields.get(0).getType(), GraphQLNonNull.nonNull(getInputType(keyDbType)));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    SchemaAssertions.assertSameTypes(fields.get(1).getType(), getInputType(valueDbType));
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
    SchemaAssertions.assertSameTypes(
        fields.get(0).getType(), GraphQLNonNull.nonNull(getOutputType(keyDbType)));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    SchemaAssertions.assertSameTypes(fields.get(1).getType(), getOutputType(valueDbType));
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
    assertThat(fields).hasSize(subTypes.length);

    for (int i = 0; i < subTypes.length; i++) {
      GraphQLFieldDefinition field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      GraphQLOutputType subType = getOutputType(subTypes[i]);
      SchemaAssertions.assertSameTypes(field.getType(), subType);
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
    assertThat(fields).hasSize(subTypes.length);

    for (int i = 0; i < subTypes.length; i++) {
      GraphQLInputObjectField field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      GraphQLInputType subType = getInputType(subTypes[i]);
      SchemaAssertions.assertSameTypes(field.getType(), subType);
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
    Stream<Arguments> builtinScalars =
        Stream.of(
            arguments(Type.Boolean, Scalars.GraphQLBoolean),
            arguments(Type.Double, Scalars.GraphQLFloat),
            arguments(Type.Int, Scalars.GraphQLInt),
            arguments(Type.Text, Scalars.GraphQLString));
    Stream<Arguments> cqlScalars =
        Arrays.stream(CqlScalar.values()).map(s -> arguments(s.getCqlType(), s.getGraphqlType()));
    return Stream.concat(builtinScalars, cqlScalars);
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
