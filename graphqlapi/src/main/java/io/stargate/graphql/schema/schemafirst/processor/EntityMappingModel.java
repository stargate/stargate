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

import com.google.common.collect.ImmutableList;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.List;
import java.util.Optional;

public class EntityMappingModel {

  public enum Target {
    TABLE,
    UDT,
    ;
  }

  private final String graphqlName;
  private final String keyspaceName;
  private final String cqlName;
  private final Target target;
  private final List<FieldMappingModel> partitionKey;
  private final List<FieldMappingModel> clusteringColumns;
  private final List<FieldMappingModel> primaryKey;
  private final List<FieldMappingModel> regularColumns;
  private final List<FieldMappingModel> allColumns;
  private final Table tableCqlSchema;
  private final UserDefinedType udtCqlSchema;
  private final boolean isFederated;
  private final Optional<String> inputTypeName;

  EntityMappingModel(
      String graphqlName,
      String keyspaceName,
      String cqlName,
      Target target,
      List<FieldMappingModel> partitionKey,
      List<FieldMappingModel> clusteringColumns,
      List<FieldMappingModel> regularColumns,
      Table tableCqlSchema,
      UserDefinedType udtCqlSchema,
      boolean isFederated,
      Optional<String> inputTypeName) {

    assert (target == Target.TABLE && tableCqlSchema != null && udtCqlSchema == null)
        || (target == Target.UDT && tableCqlSchema == null && udtCqlSchema != null);

    this.graphqlName = graphqlName;
    this.keyspaceName = keyspaceName;
    this.cqlName = cqlName;
    this.target = target;
    this.partitionKey = ImmutableList.copyOf(partitionKey);
    this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
    this.primaryKey =
        ImmutableList.<FieldMappingModel>builder()
            .addAll(partitionKey)
            .addAll(clusteringColumns)
            .build();
    this.regularColumns = ImmutableList.copyOf(regularColumns);
    this.allColumns =
        ImmutableList.<FieldMappingModel>builder()
            .addAll(primaryKey)
            .addAll(regularColumns)
            .build();
    this.tableCqlSchema = tableCqlSchema;
    this.udtCqlSchema = udtCqlSchema;
    this.isFederated = isFederated;
    this.inputTypeName = inputTypeName;
  }

  public String getGraphqlName() {
    return graphqlName;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getCqlName() {
    return cqlName;
  }

  public Target getTarget() {
    return target;
  }

  public List<FieldMappingModel> getPartitionKey() {
    return partitionKey;
  }

  public List<FieldMappingModel> getClusteringColumns() {
    return clusteringColumns;
  }

  /**
   * The full primary key (partition key + clustering columns), that uniquely identifies a CQL row.
   */
  public List<FieldMappingModel> getPrimaryKey() {
    return primaryKey;
  }

  public List<FieldMappingModel> getRegularColumns() {
    return regularColumns;
  }

  public List<FieldMappingModel> getAllColumns() {
    return allColumns;
  }

  public Table getTableCqlSchema() {
    if (target != Target.TABLE) {
      throw new UnsupportedOperationException("Can't call this method when target = " + target);
    }
    return tableCqlSchema;
  }

  public UserDefinedType getUdtCqlSchema() {
    if (target != Target.UDT) {
      throw new UnsupportedOperationException("Can't call this method when target = " + target);
    }
    return udtCqlSchema;
  }

  public boolean isFederated() {
    return isFederated;
  }

  public Optional<String> getInputTypeName() {
    return inputTypeName;
  }

  @Override
  public String toString() {
    return "EntityMappingModel{"
        + "graphqlName='"
        + graphqlName
        + '\''
        + ", keyspaceName='"
        + keyspaceName
        + '\''
        + ", cqlName='"
        + cqlName
        + '\''
        + ", target="
        + target
        + ", partitionKey="
        + partitionKey
        + ", clusteringColumns="
        + clusteringColumns
        + ", primaryKey="
        + primaryKey
        + ", regularColumns="
        + regularColumns
        + ", allColumns="
        + allColumns
        + ", tableCqlSchema="
        + tableCqlSchema
        + ", udtCqlSchema="
        + udtCqlSchema
        + ", isFederated="
        + isFederated
        + ", inputTypeName="
        + inputTypeName
        + '}';
  }
}
