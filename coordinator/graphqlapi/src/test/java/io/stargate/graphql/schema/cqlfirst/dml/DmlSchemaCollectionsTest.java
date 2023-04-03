package io.stargate.graphql.schema.cqlfirst.dml;

import static graphql.schema.GraphQLList.list;
import static io.stargate.graphql.schema.SchemaAssertions.assertField;
import static io.stargate.graphql.schema.SchemaAssertions.assertListFilter;
import static io.stargate.graphql.schema.SchemaAssertions.assertMapFilter;

import graphql.Scalars;
import graphql.schema.GraphQLInputObjectType;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.SampleKeyspaces;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DmlSchemaCollectionsTest extends DmlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.COLLECTIONS));
  }

  @Test
  @DisplayName("Should surface list and set types")
  public void listTest() {
    GraphQLInputObjectType listFilterInput =
        ((GraphQLInputObjectType) graphQlSchema.getType("ListIntFilterInput"));
    assertListFilter(listFilterInput, Scalars.GraphQLInt);
    assertField(graphQlSchema.getType("PkListTableFilterInput"), "l", listFilterInput);
    assertField(graphQlSchema.getType("RegularListTableFilterInput"), "l", listFilterInput);
    // CQL sets are translated to GraphQL lists, so these tables reuse the same type too:
    assertField(graphQlSchema.getType("PkSetTableFilterInput"), "s", listFilterInput);
    assertField(graphQlSchema.getType("RegularSetTableFilterInput"), "s", listFilterInput);
  }

  @Test
  @DisplayName("Should surface map types")
  public void mapTest() {
    GraphQLInputObjectType mapFilterInput =
        ((GraphQLInputObjectType) graphQlSchema.getType("ListEntryIntKeyStringValueFilterInput"));
    assertMapFilter(
        mapFilterInput,
        graphQlSchema.getType("EntryIntKeyStringValueInput"),
        Scalars.GraphQLInt,
        Scalars.GraphQLString);
    assertField(graphQlSchema.getType("PkMapTableFilterInput"), "m", mapFilterInput);
    assertField(graphQlSchema.getType("RegularMapTableFilterInput"), "m", mapFilterInput);
  }

  @Test
  @DisplayName("Should surface nested collections")
  public void nestedCollectionTest() {
    GraphQLInputObjectType mapFilterInput =
        ((GraphQLInputObjectType)
            graphQlSchema.getType("ListEntryIntKeyListListStringValueFilterInput"));
    assertMapFilter(
        mapFilterInput,
        graphQlSchema.getType("EntryIntKeyListListStringValueInput"),
        Scalars.GraphQLInt,
        list(list(Scalars.GraphQLString)));
    assertField(graphQlSchema.getType("NestedCollectionsFilterInput"), "c", mapFilterInput);
  }
}
