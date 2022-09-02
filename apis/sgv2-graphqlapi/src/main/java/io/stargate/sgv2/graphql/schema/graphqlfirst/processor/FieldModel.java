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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import graphql.language.Type;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.Schema.ColumnOrderBy;
import java.util.Optional;

public class FieldModel {

  private final String graphqlName;
  private final Type<?> graphqlType;
  private final String cqlName;
  private final TypeSpec cqlType;
  private final boolean partitionKey;
  private final Optional<ColumnOrderBy> clusteringOrder;
  private final Optional<IndexModel> index;

  FieldModel(
      String graphqlName,
      Type<?> graphqlType,
      String cqlName,
      TypeSpec cqlType,
      boolean partitionKey,
      Optional<ColumnOrderBy> clusteringOrder,
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
   * "shallow".
   */
  public TypeSpec getCqlType() {
    return cqlType;
  }

  public boolean isPartitionKey() {
    return partitionKey;
  }

  public FieldModel asPartitionKey() {
    return new FieldModel(graphqlName, graphqlType, cqlName, cqlType, true, clusteringOrder, index);
  }

  public Optional<ColumnOrderBy> getClusteringOrder() {
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
