package io.stargate.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.Scalars;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import io.stargate.db.schema.Keyspace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DmlSchemaBuilderUdtTest extends DmlTestBase {

  @Override
  public Keyspace getKeyspace() {
    return SampleKeyspaces.UDTS;
  }

  @Test
  @DisplayName("Should surface UDT types")
  public void udtTest() {
    // Input and output types should have been created for each UDT:
    GraphQLInputObjectType bInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("BUdtInput"));
    assertThat(bInputType.getFields()).hasSize(1);
    assertThat(bInputType.getField("i").getType()).isEqualTo(Scalars.GraphQLInt);

    GraphQLInputObjectType aInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("AUdtInput"));
    assertThat(aInputType.getFields()).hasSize(1);
    assertThat(aInputType.getField("b").getType()).isEqualTo(bInputType);

    GraphQLInputObjectType bFilterInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("BUdtFilterInput"));
    assertFilterInput(bFilterInputType, bInputType);

    GraphQLInputObjectType aFilterInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("AUdtFilterInput"));
    assertFilterInput(aFilterInputType, aInputType);

    GraphQLObjectType bOutputType = (GraphQLObjectType) graphQlSchema.getType("BUdt");
    assertThat(bOutputType.getFieldDefinitions()).hasSize(1);
    assertThat(bOutputType.getFieldDefinition("i").getType()).isEqualTo(Scalars.GraphQLInt);

    GraphQLObjectType aOutputType = (GraphQLObjectType) graphQlSchema.getType("AUdt");
    assertThat(aOutputType.getFieldDefinitions()).hasSize(1);
    assertThat(aOutputType.getFieldDefinition("b").getType()).isEqualTo(bOutputType);

    // Those types should also be referenced in query and mutation inputs and outputs:
    GraphQLInputObjectType testTableInput =
        (GraphQLInputObjectType) graphQlSchema.getType("TestTableInput");
    assertThat(testTableInput.getFieldDefinitions()).hasSize(1);
    assertThat(testTableInput.getFieldDefinition("a").getType()).isEqualTo(aInputType);

    GraphQLInputObjectType testTableFilterInput =
        (GraphQLInputObjectType) graphQlSchema.getType("TestTableFilterInput");
    assertThat(testTableFilterInput.getFieldDefinitions()).hasSize(1);
    assertThat(testTableFilterInput.getFieldDefinition("a").getType()).isEqualTo(aFilterInputType);

    GraphQLObjectType testTableOutput = (GraphQLObjectType) graphQlSchema.getType("TestTable");
    assertThat(testTableOutput.getFieldDefinitions()).hasSize(1);
    assertThat(testTableOutput.getFieldDefinition("a").getType()).isEqualTo(aOutputType);
  }

  private void assertFilterInput(
      GraphQLInputObjectType filterType, GraphQLInputObjectType elementType) {
    assertThat(filterType.getFieldDefinitions()).hasSize(7);
    assertThat(filterType.getField("eq").getType()).isEqualTo(elementType);
    assertThat(filterType.getField("gt").getType()).isEqualTo(elementType);
    assertThat(filterType.getField("gte").getType()).isEqualTo(elementType);
    assertThat(filterType.getField("in").getType()).isEqualTo(new GraphQLList(elementType));
    assertThat(filterType.getField("lt").getType()).isEqualTo(elementType);
    assertThat(filterType.getField("lte").getType()).isEqualTo(elementType);
    assertThat(filterType.getField("notEq").getType()).isEqualTo(elementType);
  }
}
