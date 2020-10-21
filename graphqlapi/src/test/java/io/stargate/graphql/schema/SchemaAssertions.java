package io.stargate.graphql.schema;

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
    assertThat(actualFilterType.getField("contains").getType()).isEqualTo(expectedElementType);
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

    assertThat(actualFilterType.getField("containsEntry").getType()).isEqualTo(entryInputType);
    assertThat(actualFilterType.getField("containsKey").getType()).isEqualTo(expectedKeyType);
    assertThat(actualFilterType.getField("contains").getType()).isEqualTo(expectedValueType);
  }

  private static void assertBasicComparisons(
      GraphQLInputObjectType actualFilterType, GraphQLType expectedValueType) {
    assertThat(actualFilterType.getField("eq").getType()).isEqualTo(expectedValueType);
    assertThat(actualFilterType.getField("gt").getType()).isEqualTo(expectedValueType);
    assertThat(actualFilterType.getField("gte").getType()).isEqualTo(expectedValueType);
    assertThat(actualFilterType.getField("in").getType()).isEqualTo(list(expectedValueType));
    assertThat(actualFilterType.getField("lt").getType()).isEqualTo(expectedValueType);
    assertThat(actualFilterType.getField("lte").getType()).isEqualTo(expectedValueType);
    assertThat(actualFilterType.getField("notEq").getType()).isEqualTo(expectedValueType);
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
      assertThat(field.getType()).isEqualTo(expectedFieldType);
    } else {
      fail("Unexpected type " + parentType);
    }
  }
}
