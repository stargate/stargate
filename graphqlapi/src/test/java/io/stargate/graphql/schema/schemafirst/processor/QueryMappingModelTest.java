/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.schemafirst.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import graphql.GraphqlErrorException;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.stargate.db.schema.ImmutableKeyspace;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryMappingModelTest {

  @Test
  public void shouldBuildAMappingModelAndQueryWithOnePrimaryKey() {
    // given
    TypeDefinitionRegistry typeDefinitionRegistry =
        new SchemaParser()
            .parse(
                "type User { id: ID! name: String username: String } "
                    + "type Query { getUser(id: ID!): User }");

    // when
    MappingModel mappingModel =
        MappingModel.build(
            typeDefinitionRegistry,
            new ProcessingContext(
                typeDefinitionRegistry, ImmutableKeyspace.builder().name("ks_1").build(), true));

    // then
    QueryMappingModel operationMappingModel =
        (QueryMappingModel) mappingModel.getOperations().get(0);
    assertThat(operationMappingModel.getCoordinates().getFieldName()).isEqualTo("getUser");
    assertThat(operationMappingModel.getCoordinates().getTypeName()).isEqualTo("Query");
    assertThat(operationMappingModel.getInputNames().get(0)).isEqualTo("id");

    EntityMappingModel entityMappingModel = mappingModel.getEntities().get("User");
    FieldMappingModel primaryKey = entityMappingModel.getPrimaryKey().get(0);
    assertThat(primaryKey.getCqlName()).isEqualTo("id");
    assertThat(entityMappingModel.getPrimaryKey().get(0).isPartitionKey()).isTrue();
  }

  @Test
  public void shouldThrowWhenCreatingAQueryForUnknownEntity() {
    // given
    TypeDefinitionRegistry typeDefinitionRegistry =
        new SchemaParser().parse("type Query { getUser(id: ID!): UserUnknown }");

    // when, then
    assertThatThrownBy(
            () ->
                MappingModel.build(
                    typeDefinitionRegistry,
                    new ProcessingContext(
                        typeDefinitionRegistry,
                        ImmutableKeyspace.builder().name("ks_1").build(),
                        true)))
        .isInstanceOf(GraphqlErrorException.class)
        .extracting(ex -> extractMappingErrors((GraphqlErrorException) ex))
        .isEqualTo(
            "Query getUser: expected the return type to be an object (or list of objects) that maps to an entity");
  }

  @Test
  public void shouldBuildAMappingModelAndQueryWithMultiplePrimaryKeys() {
    // given
    TypeDefinitionRegistry typeDefinitionRegistry =
        new SchemaParser()
            .parse(
                "type Foo {\n"
                    + "  pk1: Int! @cql_column(partitionKey: true)\n"
                    + "  pk2: Int! @cql_column(partitionKey: true)\n"
                    + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
                    + "  cc2: Int! @cql_column(clusteringOrder: DESC)\n"
                    + "}"
                    + "type Query { foo(pk1: Int!, pk2: Int!, cc1: Int!, cc2: Int!): Foo }");

    // when
    MappingModel mappingModel =
        MappingModel.build(
            typeDefinitionRegistry,
            new ProcessingContext(
                typeDefinitionRegistry, ImmutableKeyspace.builder().name("ks_1").build(), true));
    // then
    QueryMappingModel operationMappingModel =
        (QueryMappingModel) mappingModel.getOperations().get(0);
    assertThat(operationMappingModel.getCoordinates().getFieldName()).isEqualTo("foo");
    assertThat(operationMappingModel.getCoordinates().getTypeName()).isEqualTo("Query");
    assertThat(operationMappingModel.getInputNames().size()).isEqualTo(4);

    EntityMappingModel entityMappingModel = mappingModel.getEntities().get("Foo");
    assertThat(entityMappingModel.getPartitionKey().size()).isEqualTo(2);
    assertThat(entityMappingModel.getClusteringColumns().size()).isEqualTo(2);
    assertThat(entityMappingModel.getPrimaryKey().get(0).getCqlName()).isEqualTo("pk1");
    assertThat(entityMappingModel.getPrimaryKey().get(1).getCqlName()).isEqualTo("pk2");
    assertThat(entityMappingModel.getPrimaryKey().get(2).getCqlName()).isEqualTo("cc1");
    assertThat(entityMappingModel.getPrimaryKey().get(3).getCqlName()).isEqualTo("cc2");
  }

  @Test
  public void shouldThrowIfNoAllPartitionKeysAreInTheQuery() {
    // given
    TypeDefinitionRegistry typeDefinitionRegistry =
        new SchemaParser()
            .parse(
                "type Foo {\n"
                    + "  pk1: Int! @cql_column(partitionKey: true)\n"
                    + "  pk2: Int! @cql_column(partitionKey: true)\n"
                    + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
                    + "  cc2: Int! @cql_column(clusteringOrder: DESC)\n"
                    + "}"
                    + "type Query { foo(pk1: Int! ): Foo }");

    // when, then
    assertThatThrownBy(
            () ->
                MappingModel.build(
                    typeDefinitionRegistry,
                    new ProcessingContext(
                        typeDefinitionRegistry,
                        ImmutableKeyspace.builder().name("ks_1").build(),
                        true)))
        .isInstanceOf(GraphqlErrorException.class)
        .extracting(ex -> extractMappingErrors((GraphqlErrorException) ex))
        .isEqualTo(
            "Query foo: expected to have at least enough arguments to cover the partition key (2 needed, 1 provided).");
  }

  @Test
  public void shouldBuildAQueryWithPartialPrimaryKeys() {
    // given
    TypeDefinitionRegistry typeDefinitionRegistry =
        new SchemaParser()
            .parse(
                "type Foo {\n"
                    + "  pk1: Int! @cql_column(partitionKey: true)\n"
                    + "  pk2: Int! @cql_column(partitionKey: true)\n"
                    + "  cc1: Int! @cql_column(clusteringOrder: ASC)\n"
                    + "  cc2: Int! @cql_column(clusteringOrder: DESC)\n"
                    + "}"
                    + "type Query { foo1(pk1: Int!, pk2: Int!, cc1: Int!): [Foo] }");

    // when
    MappingModel mappingModel =
        MappingModel.build(
            typeDefinitionRegistry,
            new ProcessingContext(
                typeDefinitionRegistry, ImmutableKeyspace.builder().name("ks_1").build(), true));
    // then
    QueryMappingModel operationMappingModel =
        (QueryMappingModel) mappingModel.getOperations().get(0);
    assertThat(operationMappingModel.getCoordinates().getFieldName()).isEqualTo("foo1");
    assertThat(operationMappingModel.getCoordinates().getTypeName()).isEqualTo("Query");
    assertThat(operationMappingModel.getInputNames().size()).isEqualTo(3);

    EntityMappingModel entityMappingModel = mappingModel.getEntities().get("Foo");
    assertThat(entityMappingModel.getPartitionKey().size()).isEqualTo(2);
    assertThat(entityMappingModel.getClusteringColumns().size()).isEqualTo(2);
    assertThat(entityMappingModel.getPrimaryKey().get(0).getCqlName()).isEqualTo("pk1");
    assertThat(entityMappingModel.getPrimaryKey().get(1).getCqlName()).isEqualTo("pk2");
    assertThat(entityMappingModel.getPrimaryKey().get(2).getCqlName()).isEqualTo("cc1");
    assertThat(entityMappingModel.getPrimaryKey().get(3).getCqlName()).isEqualTo("cc2");
  }

  private String extractMappingErrors(GraphqlErrorException ex) {
    return ((ProcessingMessage) ((List) ex.getExtensions().get("mappingErrors")).get(0))
        .getMessage();
  }
}
