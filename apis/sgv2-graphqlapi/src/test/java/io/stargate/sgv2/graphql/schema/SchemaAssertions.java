package io.stargate.sgv2.graphql.schema;

import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;

/** Helper class for tests that introspect a {@link GraphQLSchema}. */
public class SchemaAssertions {

  private SchemaAssertions() {}

  /** Checks that the given type is a "FilterInput" for the given non-collection type. */
  public static void assertBasicComparisonsFilter(
      GraphQLInputObjectType actualFilterType, GraphQLType expectedValueType) {
    assertThat(actualFilterType.getFieldDefinitions()).hasSize(7);
    assertBasicComparisons(actualFilterType, expectedValueType);
  }

  /** Checks that the given type is a "FilterInput" for a list of the given type. */
  public static void assertListFilter(
      GraphQLInputObjectType actualFilterType, GraphQLType expectedElementType) {
    assertThat(actualFilterType.getFieldDefinitions()).hasSize(8);
    assertBasicComparisons(actualFilterType, list(expectedElementType));
    assertSameTypes(actualFilterType.getField("contains").getType(), expectedElementType);
  }

  /** Checks that the given type is a "FilterInput" for a map of the given entry type. */
  public static void assertMapFilter(
      GraphQLInputObjectType actualFilterType,
      GraphQLType expectedEntryType,
      GraphQLType expectedKeyType,
      GraphQLType expectedValueType) {

    GraphQLInputObjectType entryInputType = (GraphQLInputObjectType) expectedEntryType;
    assertThat(entryInputType.getFieldDefinitions()).hasSize(2);
    assertField(entryInputType, "key", nonNull(expectedKeyType));
    assertField(entryInputType, "value", expectedValueType);

    assertThat(actualFilterType.getFieldDefinitions()).hasSize(10);
    assertBasicComparisons(actualFilterType, list(entryInputType));

    assertSameTypes(actualFilterType.getField("containsEntry").getType(), entryInputType);
    assertSameTypes(actualFilterType.getField("containsKey").getType(), expectedKeyType);
    assertSameTypes(actualFilterType.getField("contains").getType(), expectedValueType);
  }

  private static void assertBasicComparisons(
      GraphQLInputObjectType actualFilterType, GraphQLType expectedValueType) {
    assertSameTypes(actualFilterType.getField("eq").getType(), expectedValueType);
    assertSameTypes(actualFilterType.getField("gt").getType(), expectedValueType);
    assertSameTypes(actualFilterType.getField("gte").getType(), expectedValueType);
    assertSameTypes(actualFilterType.getField("in").getType(), list(expectedValueType));
    assertSameTypes(actualFilterType.getField("lt").getType(), expectedValueType);
    assertSameTypes(actualFilterType.getField("lte").getType(), expectedValueType);
    assertSameTypes(actualFilterType.getField("notEq").getType(), expectedValueType);
  }

  /** Checks that a type contains a field with a given sub-type. */
  public static void assertField(
      GraphQLType parentType, String fieldName, GraphQLType expectedFieldType) {

    if (parentType instanceof GraphQLObjectType) {
      GraphQLObjectType objectType = (GraphQLObjectType) parentType;
      GraphQLFieldDefinition field = objectType.getFieldDefinition(fieldName);
      assertThat(field).as("No '%s' field found in %s", fieldName, objectType).isNotNull();
      assertThat(field.getType()).isEqualTo(expectedFieldType);
    } else if (parentType instanceof GraphQLInputObjectType) {
      GraphQLInputObjectType objectType = (GraphQLInputObjectType) parentType;
      GraphQLInputObjectField field = objectType.getFieldDefinition(fieldName);
      assertThat(field).as("No '%s' field found in %s", fieldName, objectType).isNotNull();
      assertSameTypes(field.getType(), expectedFieldType);
    } else {
      fail("Unexpected type " + parentType);
    }
  }

  public static void assertSameTypes(GraphQLType type1, GraphQLType type2) {
    // Can't use equals() because GraphQL types only implement strict reference equality. This is
    // good enough for our purpose:
    assertThat(GraphQLTypeUtil.simplePrint(type1)).isEqualTo(GraphQLTypeUtil.simplePrint(type2));
  }
}
