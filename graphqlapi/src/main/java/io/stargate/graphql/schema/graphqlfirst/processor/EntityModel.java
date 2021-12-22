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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.db.query.Predicate;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class EntityModel {

  public enum Target {
    TABLE,
    UDT,
  }

  private final String graphqlName;
  private final String keyspaceName;
  private final String cqlName;
  private final Target target;
  private final List<FieldModel> partitionKey;
  private final List<FieldModel> clusteringColumns;
  private final List<FieldModel> primaryKey;
  private final List<ConditionModel> primaryKeyWhereConditions;
  private final List<FieldModel> regularColumns;
  private final List<FieldModel> allColumns;
  private final Table tableCqlSchema;
  private final UserDefinedType udtCqlSchema;
  private final boolean isFederated;
  private final Optional<String> inputTypeName;

  @SuppressWarnings("PMD.ExcessiveParameterList")
  EntityModel(
      String graphqlName,
      String keyspaceName,
      String cqlName,
      Target target,
      List<FieldModel> partitionKey,
      List<FieldModel> clusteringColumns,
      List<FieldModel> regularColumns,
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
        ImmutableList.<FieldModel>builder().addAll(partitionKey).addAll(clusteringColumns).build();
    this.primaryKeyWhereConditions =
        primaryKey.stream()
            .map(field -> new ConditionModel(field, Predicate.EQ, field.getGraphqlName()))
            .collect(Collectors.toList());
    this.regularColumns = ImmutableList.copyOf(regularColumns);
    this.allColumns =
        ImmutableList.<FieldModel>builder().addAll(primaryKey).addAll(regularColumns).build();
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

  public List<FieldModel> getPartitionKey() {
    return partitionKey;
  }

  public List<FieldModel> getClusteringColumns() {
    return clusteringColumns;
  }

  /**
   * The full primary key (partition key + clustering columns), that uniquely identifies a CQL row.
   */
  public List<FieldModel> getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Returns a default list of WHERE conditions that select all primary key columns with {@code =}
   * relations.
   */
  public List<ConditionModel> getPrimaryKeyWhereConditions() {
    return primaryKeyWhereConditions;
  }

  public List<FieldModel> getRegularColumns() {
    return regularColumns;
  }

  public List<FieldModel> getAllColumns() {
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

  /**
   * Validates a set of conditions to ensure that they will produce a valid CQL query that doesn't
   * require ALLOW FILTERING for this entity.
   *
   * @return an optional error message if the validation failed, empty otherwise.
   */
  public Optional<String> validateNoFiltering(Collection<ConditionModel> conditions) {

    long indexCount = conditions.stream().filter(c -> !c.getField().isPrimaryKey()).count();

    if (indexCount == 0) {
      return validateNoFilteringWhenNoIndex(conditions);
    }
    if (indexCount == 1) {
      return validateNoFilteringWhenOneIndex(conditions);
    }
    return Optional.of("cannot use more than one indexed field per operation");
  }

  private Optional<String> validateNoFilteringWhenNoIndex(Collection<ConditionModel> conditions) {
    // All partition key fields must be present
    for (FieldModel field : this.getPartitionKey()) {
      if (conditions.stream().noneMatch(c -> c.getField().equals(field))) {
        return Optional.of(
            String.format(
                "every partition key field of type %s must be present (expected: %s).",
                this.getGraphqlName(), describePartitionKey()));
      }
    }

    // When a clustering field is NOT restricted by an equality relation, then no other clustering
    // field after it can be restricted by any condition.
    String firstNonEqField = null;
    for (FieldModel field : this.getClusteringColumns()) {
      Optional<ConditionModel> maybeCondition =
          conditions.stream().filter(c -> c.getField().equals(field)).findFirst();
      if (!maybeCondition.isPresent()) {
        if (firstNonEqField == null) {
          firstNonEqField = field.getGraphqlName();
        }
      } else {
        if (firstNonEqField != null) {
          return Optional.of(
              String.format(
                  "clustering field %s is not restricted by EQ or IN, "
                      + "so no other clustering field after it can be restricted (offending: %s).",
                  firstNonEqField, field.getGraphqlName()));
        }
        Predicate predicate = maybeCondition.get().getPredicate();
        if (predicate != Predicate.EQ && predicate != Predicate.IN) {
          firstNonEqField = field.getGraphqlName();
        }
      }
    }
    return Optional.empty();
  }

  private Optional<String> validateNoFilteringWhenOneIndex(Collection<ConditionModel> conditions) {

    // No clustering fields
    if (conditions.stream().anyMatch(c -> c.getField().isClusteringColumn())) {
      return Optional.of("when an indexed field is present, no clustering field can be used");
    }

    // Either no partition key fields, or all of them with equality restrictions
    long partitionKeyCount =
        this.getPartitionKey().stream()
            .filter(field -> conditions.stream().anyMatch(c -> c.getField().equals(field)))
            .count();
    if (partitionKeyCount > 0 && partitionKeyCount != this.getPartitionKey().size()) {
      return Optional.of(
          String.format(
              "when an indexed field is present, either none or all of the partition key "
                  + "fields must be present (expected %s).",
              describePartitionKey()));
    }

    return Optional.empty();
  }

  private String describePartitionKey() {
    return this.getPartitionKey().stream()
        .map(FieldModel::getGraphqlName)
        .collect(Collectors.joining(", "));
  }

  /**
   * Validates a set of conditions to ensure that they form a valid WHERE clause for an UPDATE.
   *
   * @return an optional error message if the validation failed, empty otherwise.
   */
  public Optional<String> validateForUpdate(Collection<ConditionModel> conditions) {
    long primaryKeyCount =
        getPrimaryKey().stream()
            .filter(
                field ->
                    conditions.stream()
                        .anyMatch(
                            (c ->
                                c.getField().equals(field)
                                    && (c.getPredicate() == Predicate.EQ
                                        || c.getPredicate() == Predicate.IN))))
            .count();
    if (primaryKeyCount != getPrimaryKey().size()) {
      return Optional.of(
          String.format(
              "all of the primary key fields must be restricted by EQ or IN predicates (expected %s).",
              describePrimaryKey()));
    }
    return Optional.empty();
  }

  private String describePrimaryKey() {
    return this.getPrimaryKey().stream()
        .map(FieldModel::getGraphqlName)
        .collect(Collectors.joining(", "));
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
