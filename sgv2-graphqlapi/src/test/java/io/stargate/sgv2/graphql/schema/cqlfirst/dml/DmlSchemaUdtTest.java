package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import static io.stargate.sgv2.graphql.schema.SchemaAssertions.assertBasicComparisonsFilter;
import static io.stargate.sgv2.graphql.schema.SchemaAssertions.assertField;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import graphql.Scalars;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import io.stargate.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DmlSchemaUdtTest extends DmlTestBase {

  @Override
  protected List<Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.UDTS);
  }

  @Test
  @DisplayName("Should surface UDT types")
  public void udtTest() {
    // Input and output types should have been created for each UDT:
    GraphQLInputObjectType bInputType =
        ((GraphQLInputObjectType) graphqlSchema.getType("BUdtInput"));
    assertThat(bInputType.getFields()).hasSize(1);
    assertField(bInputType, "i", Scalars.GraphQLInt);

    GraphQLInputObjectType aInputType =
        ((GraphQLInputObjectType) graphqlSchema.getType("AUdtInput"));
    assertThat(aInputType.getFields()).hasSize(1);
    assertField(aInputType, "b", bInputType);

    // Note that there is no BUdtFilterInput, because it is not referenced directly by a column,
    // only as a nested field of a.

    GraphQLInputObjectType aFilterInputType =
        ((GraphQLInputObjectType) graphqlSchema.getType("AUdtFilterInput"));
    assertBasicComparisonsFilter(aFilterInputType, aInputType);

    GraphQLObjectType bOutputType = (GraphQLObjectType) graphqlSchema.getType("BUdt");
    assertThat(bOutputType.getFieldDefinitions()).hasSize(1);
    assertField(bOutputType, "i", Scalars.GraphQLInt);

    GraphQLObjectType aOutputType = (GraphQLObjectType) graphqlSchema.getType("AUdt");
    assertThat(aOutputType.getFieldDefinitions()).hasSize(1);
    assertField(aOutputType, "b", bOutputType);

    // Those types should also be referenced in query and mutation inputs and outputs:
    assertField(graphqlSchema.getType("TestTableInput"), "a", aInputType);
    assertField(graphqlSchema.getType("TestTableFilterInput"), "a", aFilterInputType);
    assertField(graphqlSchema.getType("TestTable"), "a", aOutputType);
  }
}
