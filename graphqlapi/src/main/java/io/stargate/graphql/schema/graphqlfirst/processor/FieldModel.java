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
package io.stargate.graphql.schema.graphqlfirst.processor;

import graphql.language.Type;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Order;
import java.util.Optional;

public class FieldModel {

  private final String graphqlName;
  private final Type<?> graphqlType;
  private final String cqlName;
  private final ColumnType cqlType;
  private final boolean partitionKey;
  private final Optional<Order> clusteringOrder;
  private final Optional<IndexModel> index;

  FieldModel(
      String graphqlName,
      Type<?> graphqlType,
      String cqlName,
      ColumnType cqlType,
      boolean partitionKey,
      Optional<Order> clusteringOrder,
      Optional<IndexModel> index) {
    this.graphqlName = graphqlName;
    this.graphqlType = graphqlType;
    this.cqlName = cqlName;
    this.cqlType = cqlType;
    this.partitionKey = partitionKey;
    this.clusteringOrder = clusteringOrder;
    this.index = index;
  }

  public String getGraphqlName() {
    return graphqlName;
  }

  public Type<?> getGraphqlType() {
    return graphqlType;
  }

  public String getCqlName() {
    return cqlName;
  }

  /**
   * Note that if this type references any UDTs (either directly or through subtypes), they might be
   * "shallow", as defined by {@link Column.Type#fromCqlDefinitionOf(io.stargate.db.schema.Keyspace,
   * java.lang.String, boolean)}.
   */
  public ColumnType getCqlType() {
    return cqlType;
  }

  public boolean isPartitionKey() {
    return partitionKey;
  }

  public FieldModel asPartitionKey() {
    return new FieldModel(graphqlName, graphqlType, cqlName, cqlType, true, clusteringOrder, index);
  }

  public Optional<Order> getClusteringOrder() {
    return clusteringOrder;
  }

  public boolean isClusteringColumn() {
    return getClusteringOrder().isPresent();
  }

  public boolean isPrimaryKey() {
    return partitionKey || clusteringOrder.isPresent();
  }

  public Optional<IndexModel> getIndex() {
    return index;
  }
}
