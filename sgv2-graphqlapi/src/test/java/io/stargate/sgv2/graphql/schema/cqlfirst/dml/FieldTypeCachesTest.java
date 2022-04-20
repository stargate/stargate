package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

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
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.sgv2.graphql.schema.SchemaAssertions;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
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
public class FieldTypeCachesTest extends TypeCacheTestBase {
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
      TypeSpec.Builder dbType, GraphQLScalarType gqlType) {
    SchemaAssertions.assertSameTypes(getInputType(dbType.build()), gqlType);
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportList(
      TypeSpec.Builder scalarDbType, GraphQLScalarType gqlType) {
    TypeSpec listType = listT(scalarDbType).build();
    SchemaAssertions.assertSameTypes(getInputType(listType), new GraphQLList(gqlType));
  }

  @ParameterizedTest
  @MethodSource("getScalarTypes")
  public void getGraphQLTypeShouldSupportSet(
      TypeSpec.Builder scalarDbType, GraphQLScalarType gqlType) {
    TypeSpec listType = listT(scalarDbType).build();
    SchemaAssertions.assertSameTypes(getInputType(listType), new GraphQLList(gqlType));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldSupportMapInput(
      TypeSpec.Builder keyDbType, TypeSpec.Builder valueDbType, String name) {
    TypeSpec mapDbType = mapT(keyDbType, valueDbType).build();
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
        fields.get(0).getType(), GraphQLNonNull.nonNull(getInputType(keyDbType.build())));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    SchemaAssertions.assertSameTypes(fields.get(1).getType(), getInputType(valueDbType.build()));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldSupportMapOutput(
      TypeSpec.Builder keyDbType, TypeSpec.Builder valueDbType, String name) {
    TypeSpec mapDbType = mapT(keyDbType, valueDbType).build();
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
        fields.get(0).getType(), GraphQLNonNull.nonNull(getOutputType(keyDbType.build())));
    assertThat(fields.get(1).getName()).isEqualTo("value");
    SchemaAssertions.assertSameTypes(fields.get(1).getType(), getOutputType(valueDbType.build()));
  }

  @ParameterizedTest
  @MethodSource("getMapArgs")
  public void getGraphQLTypeShouldReuseTheSameInstanceForMaps(
      TypeSpec.Builder keyDbType, TypeSpec.Builder valueDbType) {
    TypeSpec mapDbType = mapT(keyDbType, valueDbType).build();
    GraphQLType graphTypeParentType = getOutputType(mapDbType);
    assertThat(graphTypeParentType).isInstanceOf(GraphQLList.class);
    GraphQLSchemaElement childObjectType = graphTypeParentType.getChildren().get(0);

    // Following calls should yield the same instance
    assertThat(childObjectType).isSameAs(getOutputType(mapDbType).getChildren().get(0));
  }

  @ParameterizedTest
  @MethodSource("getMapNestedArgs")
  public void getGraphQLTypeShouldSupportMapNestedInput(
      TypeSpec.Builder keyDbType, TypeSpec.Builder valueDbType, String nameInput) {

    TypeSpec mapDbType = mapT(keyDbType, valueDbType).build();
    testNestedMaps(getInputType(mapDbType), nameInput);
  }

  @ParameterizedTest
  @MethodSource("getMapNestedArgs")
  public void getGraphQLTypeShouldSupportMapNestedOutput(
      TypeSpec.Builder keyDbType,
      TypeSpec.Builder valueDbType,
      @SuppressWarnings("unused") String nameInput,
      String nameOutput) {

    TypeSpec mapDbType = mapT(keyDbType, valueDbType).build();
    testNestedMaps(getOutputType(mapDbType), nameOutput);
  }

  @ParameterizedTest
  @MethodSource("getTupleArgs")
  public void shouldSupportTupleAsOutputType(TypeSpec.Builder[] subTypes, String name) {
    GraphQLType type = getOutputType(tupleT(subTypes).build());
    assertThat(type).isInstanceOf(GraphQLObjectType.class);

    GraphQLObjectType objectType = (GraphQLObjectType) type;
    assertThat(objectType.getName()).matches(String.format("^Tuple%s$", name));

    List<GraphQLFieldDefinition> fields = objectType.getFieldDefinitions();
    assertThat(fields).hasSize(subTypes.length);

    for (int i = 0; i < subTypes.length; i++) {
      GraphQLFieldDefinition field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      GraphQLOutputType subType = getOutputType(subTypes[i].build());
      SchemaAssertions.assertSameTypes(field.getType(), subType);
    }
  }

  @ParameterizedTest
  @MethodSource("getTupleArgs")
  public void shouldSupportTupleAsInputType(TypeSpec.Builder[] subTypes, String name) {
    GraphQLType type = getInputType(tupleT(subTypes).build());
    assertThat(type).isInstanceOf(GraphQLInputObjectType.class);

    GraphQLInputObjectType objectType = (GraphQLInputObjectType) type;
    assertThat(objectType.getName()).matches(String.format("^Tuple%sInput$", name));

    List<GraphQLInputObjectField> fields = objectType.getFieldDefinitions();
    assertThat(fields).hasSize(subTypes.length);

    for (int i = 0; i < subTypes.length; i++) {
      GraphQLInputObjectField field = fields.get(i);
      assertThat(field.getName()).isEqualTo("item" + i);
      GraphQLInputType subType = getInputType(subTypes[i].build());
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
            arguments(BOOLEAN_TYPE, Scalars.GraphQLBoolean),
            arguments(DOUBLE_TYPE, Scalars.GraphQLFloat),
            arguments(INT_TYPE, Scalars.GraphQLInt),
            arguments(TEXT_TYPE, Scalars.GraphQLString));
    Stream<Arguments> cqlScalars =
        Arrays.stream(CqlScalar.values())
            .map(
                s -> arguments(TypeSpec.newBuilder().setBasic(s.getCqlType()), s.getGraphqlType()));
    return Stream.concat(builtinScalars, cqlScalars);
  }

  public static Stream<Arguments> getMapArgs() {
    return Stream.of(
        arguments(TEXT_TYPE, UUID_TYPE, "EntryStringKeyUuidValue"),
        arguments(INT_TYPE, TIMEUUID_TYPE, "EntryIntKeyTimeUuidValue"),
        arguments(TIMEUUID_TYPE, BIGINT_TYPE, "EntryTimeUuidKeyBigIntValue"),
        arguments(TIMESTAMP_TYPE, INET_TYPE, "EntryTimestampKeyInetValue"),
        arguments(UUID_TYPE, listT(FLOAT_TYPE), "EntryUuidKeyListFloat32Value"),
        arguments(SMALLINT_TYPE, setT(DOUBLE_TYPE), "EntrySmallIntKeyListFloatValue"));
  }

  public static Stream<Arguments> getMapNestedArgs() {
    return Stream.of(
        arguments(
            TINYINT_TYPE,
            mapT(UUID_TYPE, INT_TYPE),
            "EntryTinyIntKeyListEntryUuidKeyIntValueInputValueInput",
            "EntryTinyIntKeyListEntryUuidKeyIntValueValue"),
        arguments(
            mapT(TEXT_TYPE, BIGINT_TYPE),
            setT(DOUBLE_TYPE),
            "EntryListEntryStringKeyBigIntValueInputKeyListFloatValueInput",
            "EntryListEntryStringKeyBigIntValueKeyListFloatValue"));
  }

  public static Stream<Arguments> getTupleArgs() {
    return Stream.of(
        arguments(new TypeSpec.Builder[] {TEXT_TYPE, UUID_TYPE}, "StringUuid"),
        arguments(new TypeSpec.Builder[] {UUID_TYPE, setT(DOUBLE_TYPE)}, "UuidListFloat"),
        arguments(new TypeSpec.Builder[] {INT_TYPE, TIMEUUID_TYPE}, "IntTimeUuid"),
        arguments(new TypeSpec.Builder[] {DOUBLE_TYPE}, "Float"),
        arguments(
            new TypeSpec.Builder[] {UUID_TYPE, DECIMAL_TYPE, TIMESTAMP_TYPE},
            "UuidDecimalTimestamp"),
        arguments(new TypeSpec.Builder[] {FLOAT_TYPE, FLOAT_TYPE}, "Float32Float32"));
  }

  /** Gets a GraphQL input type using the shared cache */
  private GraphQLInputType getInputType(TypeSpec dbType) {
    GraphQLInputType type = fieldInputTypes.get(dbType);
    assertThat(type).isInstanceOf(GraphQLInputType.class);
    return type;
  }

  /** Gets a GraphQL output type using the shared cache */
  private GraphQLOutputType getOutputType(TypeSpec dbType) {
    GraphQLOutputType type = fieldOutputTypes.get(dbType);
    assertThat(type).isInstanceOf(GraphQLOutputType.class);
    return type;
  }
}
