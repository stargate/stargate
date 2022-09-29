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
package io.stargate.graphql.schema.graphqlfirst.migration;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import graphql.GraphqlErrorException;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.SchemaEntity;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.graphqlfirst.migration.CassandraSchemaHelper.Difference;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.util.DirectedGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraMigrator {

  private static final BiFunction<UserDefinedType, SecondaryIndex, MigrationQuery>
      UDT_NO_CREATE_INDEX =
          (table, index) -> {
            throw new AssertionError(
                "This should never get invoked since UDT fields can't be indexed");
          };

  private final MigrationStrategy strategy;

  private final boolean isPersisted;

  /** Creates a new instance for a deployment initiated by the user. */
  public static CassandraMigrator forDeployment(MigrationStrategy strategy) {
    return new CassandraMigrator(strategy, false);
  }

  /**
   * Creates a new instance for a GraphQL schema already stored in the database (from a previous
   * deployment).
   */
  public static CassandraMigrator forPersisted() {
    return new CassandraMigrator(MigrationStrategy.USE_EXISTING, true);
  }

  private CassandraMigrator(MigrationStrategy strategy, boolean isPersisted) {
    this.strategy = strategy;
    this.isPersisted = isPersisted;
  }

  /** @throws GraphqlErrorException if the CQL data model can't be migrated */
  public List<MigrationQuery> compute(MappingModel mappingModel, Keyspace keyspace) {
    List<MigrationQuery> queries = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    for (EntityModel entity : mappingModel.getEntities().values()) {
      switch (entity.getTarget()) {
        case TABLE:
          Table expectedTable = entity.getTableCqlSchema();
          Table actualTable = keyspace.table(entity.getCqlName());
          compute(
              expectedTable,
              actualTable,
              CassandraSchemaHelper::compare,
              CreateTableQuery::createTableAndIndexes,
              DropTableQuery::new,
              AddTableColumnQuery::new,
              CreateIndexQuery::new,
              "table",
              queries,
              errors);
          break;
        case UDT:
          UserDefinedType expectedType = entity.getUdtCqlSchema();
          UserDefinedType actualType = keyspace.userDefinedType(entity.getCqlName());
          compute(
              expectedType,
              actualType,
              CassandraSchemaHelper::compare,
              CreateUdtQuery::createUdt,
              DropUdtQuery::new,
              AddUdtFieldQuery::new,
              UDT_NO_CREATE_INDEX,
              "UDT",
              queries,
              errors);
          break;
        default:
          throw new AssertionError("Unexpected target " + entity.getTarget());
      }
    }
    if (!errors.isEmpty()) {
      String message =
          isPersisted
              ? "The GraphQL schema stored for this keyspace doesn't match the CQL data model "
                  + "anymore. It looks like the database was altered manually."
              : String.format(
                  "The GraphQL schema that you provided can't be mapped to the current CQL data "
                      + "model. Consider using a different migration strategy (current: %s).",
                  strategy);
      throw GraphqlErrorException.newErrorException()
          .message(message + " See details in `extensions.migrationErrors` below.")
          .extensions(ImmutableMap.of("migrationErrors", errors))
          .build();
    }
    return sortForExecution(queries);
  }

  @SuppressWarnings("PMD.ExcessiveParameterList")
  private <T extends SchemaEntity> void compute(
      T expected,
      T actual,
      BiFunction<T, T, List<Difference>> comparator,
      Function<T, List<MigrationQuery>> createBuilder,
      Function<T, MigrationQuery> dropBuilder,
      BiFunction<T, Column, MigrationQuery> addColumnBuilder,
      BiFunction<T, SecondaryIndex, MigrationQuery> createIndexBuilder,
      String entityType,
      List<MigrationQuery> queries,
      List<String> errors) {

    switch (strategy) {
      case USE_EXISTING:
        if (actual == null) {
          errors.add(String.format("Missing %s %s", entityType, expected.name()));
        } else {
          failIfMismatch(expected, actual, comparator, errors);
        }
        break;
      case ADD_MISSING_TABLES:
        if (actual == null) {
          queries.addAll(createBuilder.apply(expected));
        } else {
          failIfMismatch(expected, actual, comparator, errors);
        }
        break;
      case ADD_MISSING_TABLES_AND_COLUMNS:
        if (actual == null) {
          queries.addAll(createBuilder.apply(expected));
        } else {
          addMissingColumns(
              expected, actual, comparator, addColumnBuilder, createIndexBuilder, queries, errors);
        }
        break;
      case DROP_AND_RECREATE_ALL:
        queries.add(dropBuilder.apply(expected));
        queries.addAll(createBuilder.apply(expected));
        break;
      case DROP_AND_RECREATE_IF_MISMATCH:
        if (actual == null) {
          queries.addAll(createBuilder.apply(expected));
        } else {
          List<Difference> differences = comparator.apply(expected, actual);
          if (!differences.isEmpty()) {
            queries.add(dropBuilder.apply(expected));
            queries.addAll(createBuilder.apply(expected));
          }
        }
        break;
      default:
        throw new AssertionError("Unexpected strategy " + strategy);
    }
  }

  private <T extends SchemaEntity> void failIfMismatch(
      T expected, T actual, BiFunction<T, T, List<Difference>> comparator, List<String> errors) {
    List<Difference> differences = comparator.apply(expected, actual);
    if (!differences.isEmpty()) {
      errors.addAll(
          differences.stream().map(Difference::toGraphqlMessage).collect(Collectors.toList()));
    }
  }

  private <T extends SchemaEntity> void addMissingColumns(
      T expected,
      T actual,
      BiFunction<T, T, List<Difference>> comparator,
      BiFunction<T, Column, MigrationQuery> addColumnBuilder,
      BiFunction<T, SecondaryIndex, MigrationQuery> createIndexBuilder,
      List<MigrationQuery> queries,
      List<String> errors) {
    List<Difference> differences = comparator.apply(expected, actual);
    if (!differences.isEmpty()) {
      List<Difference> blockers =
          differences.stream().filter(d -> !isAddableColumn(d)).collect(Collectors.toList());
      if (blockers.isEmpty()) {
        for (Difference difference : differences) {
          assert isAddableColumn(difference);
          if (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_COLUMN) {
            queries.add(addColumnBuilder.apply(expected, difference.getColumn()));
          } else if (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_INDEX) {
            queries.add(createIndexBuilder.apply(expected, difference.getIndex()));
          }
        }
      } else {
        errors.addAll(
            blockers.stream().map(Difference::toGraphqlMessage).collect(Collectors.toList()));
      }
    }
  }

  private boolean isAddableColumn(Difference difference) {
    return (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_COLUMN
            || difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_INDEX)
        && !difference.getColumn().isPartitionKey()
        && !difference.getColumn().isClusteringKey();
  }

  @VisibleForTesting
  static List<MigrationQuery> sortForExecution(List<MigrationQuery> queries) {
    if (queries.size() < 2) {
      return queries;
    }
    DirectedGraph<MigrationQuery> graph = new DirectedGraph<>(queries);
    for (MigrationQuery query1 : queries) {
      for (MigrationQuery query2 : queries) {
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        boolean isSame = query1 != query2;
        if (isSame && query1.mustRunBefore(query2)) {
          graph.addEdge(query1, query2);
        }
      }
    }
    return graph.topologicalSort();
  }
}
