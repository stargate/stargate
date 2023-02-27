package io.stargate.graphql.schema.cqlfirst.dml;

import static io.stargate.graphql.schema.SchemaAssertions.assertBasicComparisonsFilter;
import static io.stargate.graphql.schema.SchemaAssertions.assertField;
import static org.assertj.core.api.Assertions.assertThat;

import graphql.Scalars;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import io.stargate.db.schema.Schema;
import io.stargate.graphql.schema.SampleKeyspaces;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DmlSchemaUdtTest extends DmlTestBase {

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(SampleKeyspaces.UDTS));
  }

  @Test
  @DisplayName("Should surface UDT types")
  public void udtTest() {
    // Input and output types should have been created for each UDT:
    GraphQLInputObjectType bInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("BUdtInput"));
    assertThat(bInputType.getFields()).hasSize(1);
    assertField(bInputType, "i", Scalars.GraphQLInt);

    GraphQLInputObjectType aInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("AUdtInput"));
    assertThat(aInputType.getFields()).hasSize(1);
    assertField(aInputType, "b", bInputType);

    // Note that there is no BUdtFilterInput, because it is not referenced directly by a column,
    // only as a nested field of a.

    GraphQLInputObjectType aFilterInputType =
        ((GraphQLInputObjectType) graphQlSchema.getType("AUdtFilterInput"));
    assertBasicComparisonsFilter(aFilterInputType, aInputType);

    GraphQLObjectType bOutputType = (GraphQLObjectType) graphQlSchema.getType("BUdt");
    assertThat(bOutputType.getFieldDefinitions()).hasSize(1);
    assertField(bOutputType, "i", Scalars.GraphQLInt);

    GraphQLObjectType aOutputType = (GraphQLObjectType) graphQlSchema.getType("AUdt");
    assertThat(aOutputType.getFieldDefinitions()).hasSize(1);
    assertField(aOutputType, "b", bOutputType);

    // Those types should also be referenced in query and mutation inputs and outputs:
    assertField(graphQlSchema.getType("TestTableInput"), "a", aInputType);
    assertField(graphQlSchema.getType("TestTableFilterInput"), "a", aFilterInputType);
    assertField(graphQlSchema.getType("TestTable"), "a", aOutputType);
  }
}
