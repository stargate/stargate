package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import static io.stargate.sgv2.graphql.schema.SchemaAssertions.assertField;
import static io.stargate.sgv2.graphql.schema.SchemaAssertions.assertListFilter;
import static io.stargate.sgv2.graphql.schema.SchemaAssertions.assertMapFilter;

import com.google.common.collect.ImmutableList;
import graphql.Scalars;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import io.stargate.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DmlSchemaCollectionsTest extends DmlTestBase {

  @Override
  protected List<Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.COLLECTIONS);
  }

  @Test
  @DisplayName("Should surface list and set types")
  public void listTest() {
    GraphQLInputObjectType listFilterInput =
        ((GraphQLInputObjectType) graphqlSchema.getType("ListIntFilterInput"));
    assertListFilter(listFilterInput, Scalars.GraphQLInt);
    assertField(graphqlSchema.getType("PkListTableFilterInput"), "l", listFilterInput);
    assertField(graphqlSchema.getType("RegularListTableFilterInput"), "l", listFilterInput);
    // CQL sets are translated to GraphQL lists, so these tables reuse the same type too:
    assertField(graphqlSchema.getType("PkSetTableFilterInput"), "s", listFilterInput);
    assertField(graphqlSchema.getType("RegularSetTableFilterInput"), "s", listFilterInput);
  }

  @Test
  @DisplayName("Should surface map types")
  public void mapTest() {
    GraphQLInputObjectType mapFilterInput =
        ((GraphQLInputObjectType) graphqlSchema.getType("ListEntryIntKeyStringValueFilterInput"));
    assertMapFilter(
        mapFilterInput,
        graphqlSchema.getType("EntryIntKeyStringValueInput"),
        Scalars.GraphQLInt,
        Scalars.GraphQLString);
    assertField(graphqlSchema.getType("PkMapTableFilterInput"), "m", mapFilterInput);
    assertField(graphqlSchema.getType("RegularMapTableFilterInput"), "m", mapFilterInput);
  }

  @Test
  @DisplayName("Should surface nested collections")
  public void nestedCollectionTest() {
    GraphQLInputObjectType mapFilterInput =
        ((GraphQLInputObjectType)
            graphqlSchema.getType("ListEntryIntKeyListListStringValueFilterInput"));
    assertMapFilter(
        mapFilterInput,
        graphqlSchema.getType("EntryIntKeyListListStringValueInput"),
        Scalars.GraphQLInt,
        GraphQLList.list(GraphQLList.list(Scalars.GraphQLString)));
    assertField(graphqlSchema.getType("NestedCollectionsFilterInput"), "c", mapFilterInput);
  }
}
