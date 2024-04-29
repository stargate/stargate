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
package io.stargate.sgv2.graphql.schema.graphqlfirst.migration;

import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorException;
import graphql.VisibleForTesting;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.CassandraSchemaHelper.Difference;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.DirectedGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CassandraMigrator {

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

  /**
   * @throws GraphqlErrorException if the CQL data model can't be migrated
   */
  public List<MigrationQuery> compute(MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    List<MigrationQuery> queries = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    for (EntityModel entity : mappingModel.getEntities().values()) {
      switch (entity.getTarget()) {
        case TABLE:
          CqlTable expectedTable = entity.getTableCqlSchema();
          Optional<CqlTable> actualTable =
              keyspace.getTablesList().stream()
                  .filter(t -> t.getName().equals(entity.getCqlName()))
                  .findFirst();
          compute(expectedTable, actualTable, keyspace.getCqlKeyspace().getName(), queries, errors);
          break;
        case UDT:
          Udt expectedType = entity.getUdtCqlSchema();
          Optional<Udt> actualType =
              keyspace.getTypesList().stream()
                  .filter(t -> t.getName().equals(entity.getCqlName()))
                  .findFirst();
          compute(expectedType, actualType, keyspace.getCqlKeyspace().getName(), queries, errors);
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

  private void compute(
      CqlTable expected,
      Optional<CqlTable> maybeActual,
      String keyspaceName,
      List<MigrationQuery> queries,
      List<String> errors) {
    switch (strategy) {
      case USE_EXISTING:
        if (maybeActual.isPresent()) {
          failIfMismatch(expected, maybeActual.get(), errors);
        } else {
          errors.add(String.format("Missing table %s", expected.getName()));
        }
        break;
      case ADD_MISSING_TABLES:
        if (maybeActual.isPresent()) {
          failIfMismatch(expected, maybeActual.get(), errors);
        } else {
          queries.addAll(CreateTableQuery.createTableAndIndexes(keyspaceName, expected));
        }
        break;
      case ADD_MISSING_TABLES_AND_COLUMNS:
        if (maybeActual.isPresent()) {
          addMissingColumns(expected, maybeActual.get(), keyspaceName, queries, errors);
        } else {
          queries.addAll(CreateTableQuery.createTableAndIndexes(keyspaceName, expected));
        }
        break;
      case DROP_AND_RECREATE_ALL:
        queries.add(new DropTableQuery(keyspaceName, expected));
        queries.addAll(CreateTableQuery.createTableAndIndexes(keyspaceName, expected));
        break;
      case DROP_AND_RECREATE_IF_MISMATCH:
        if (maybeActual.isPresent()) {
          List<Difference> differences = CassandraSchemaHelper.compare(expected, maybeActual.get());
          if (!differences.isEmpty()) {
            queries.add(new DropTableQuery(keyspaceName, expected));
            queries.addAll(CreateTableQuery.createTableAndIndexes(keyspaceName, expected));
          }
        } else {
          queries.addAll(CreateTableQuery.createTableAndIndexes(keyspaceName, expected));
        }
        break;
      default:
        throw new AssertionError("Unexpected strategy " + strategy);
    }
  }

  private void failIfMismatch(CqlTable expected, CqlTable actual, List<String> errors) {
    List<Difference> differences = CassandraSchemaHelper.compare(expected, actual);
    if (!differences.isEmpty()) {
      errors.addAll(
          differences.stream().map(Difference::toGraphqlMessage).collect(Collectors.toList()));
    }
  }

  private void addMissingColumns(
      CqlTable expected,
      CqlTable actual,
      String keyspaceName,
      List<MigrationQuery> queries,
      List<String> errors) {
    List<Difference> differences = CassandraSchemaHelper.compare(expected, actual);
    if (!differences.isEmpty()) {
      List<Difference> blockers =
          differences.stream().filter(d -> !isAddableColumn(d)).collect(Collectors.toList());
      if (blockers.isEmpty()) {
        for (Difference difference : differences) {
          assert isAddableColumn(difference);
          if (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_COLUMN) {
            queries.add(
                new AddTableColumnQuery(keyspaceName, expected.getName(), difference.getColumn()));
          } else if (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_INDEX) {
            queries.add(
                new CreateIndexQuery(keyspaceName, expected.getName(), difference.getIndex()));
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
        && difference.getColumn().getKind() != Column.Kind.PARTITION_KEY
        && difference.getColumn().getKind() != Column.Kind.CLUSTERING;
  }

  private void compute(
      Udt expected,
      Optional<Udt> maybeActual,
      String keyspaceName,
      List<MigrationQuery> queries,
      List<String> errors) {

    switch (strategy) {
      case USE_EXISTING:
        if (maybeActual.isPresent()) {
          failIfMismatch(expected, maybeActual.get(), errors);
        } else {
          errors.add(String.format("Missing UDT %s", expected.getName()));
        }
        break;
      case ADD_MISSING_TABLES:
        if (maybeActual.isPresent()) {
          failIfMismatch(expected, maybeActual.get(), errors);
        } else {
          queries.add(new CreateUdtQuery(keyspaceName, expected));
        }
        break;
      case ADD_MISSING_TABLES_AND_COLUMNS:
        if (maybeActual.isPresent()) {
          addMissingColumns(expected, maybeActual.get(), keyspaceName, queries, errors);
        } else {
          queries.add(new CreateUdtQuery(keyspaceName, expected));
        }
        break;
      case DROP_AND_RECREATE_ALL:
        queries.add(new DropUdtQuery(keyspaceName, expected));
        queries.add(new CreateUdtQuery(keyspaceName, expected));
        break;
      case DROP_AND_RECREATE_IF_MISMATCH:
        if (maybeActual.isPresent()) {
          List<Difference> differences = CassandraSchemaHelper.compare(expected, maybeActual.get());
          if (!differences.isEmpty()) {
            queries.add(new DropUdtQuery(keyspaceName, expected));
            queries.add(new CreateUdtQuery(keyspaceName, expected));
          }
        } else {
          queries.add(new CreateUdtQuery(keyspaceName, expected));
        }
        break;
      default:
        throw new AssertionError("Unexpected strategy " + strategy);
    }
  }

  private void failIfMismatch(Udt expected, Udt actual, List<String> errors) {
    List<Difference> differences = CassandraSchemaHelper.compare(expected, actual);
    if (!differences.isEmpty()) {
      errors.addAll(
          differences.stream().map(Difference::toGraphqlMessage).collect(Collectors.toList()));
    }
  }

  private void addMissingColumns(
      Udt expected,
      Udt actual,
      String keyspaceName,
      List<MigrationQuery> queries,
      List<String> errors) {
    List<Difference> differences = CassandraSchemaHelper.compare(expected, actual);
    if (!differences.isEmpty()) {
      List<Difference> blockers =
          differences.stream().filter(d -> !isAddableColumn(d)).collect(Collectors.toList());
      if (blockers.isEmpty()) {
        for (Difference difference : differences) {
          assert isAddableColumn(difference);
          if (difference.getType() == CassandraSchemaHelper.DifferenceType.MISSING_COLUMN) {
            queries.add(
                new AddUdtFieldQuery(
                    keyspaceName,
                    expected.getName(),
                    difference.getColumn().getSpec().getName(),
                    difference.getColumn().getSpec().getType()));
          }
        }
      } else {
        errors.addAll(
            blockers.stream().map(Difference::toGraphqlMessage).collect(Collectors.toList()));
      }
    }
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
