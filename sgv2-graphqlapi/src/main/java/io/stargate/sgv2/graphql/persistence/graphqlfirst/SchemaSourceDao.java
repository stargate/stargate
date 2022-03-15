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
package io.stargate.sgv2.graphql.persistence.graphqlfirst;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.proto.Rows;
import io.stargate.sgv2.graphql.schema.Uuids;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaSourceDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSourceDao.class);
  public static final String KEYSPACE_NAME = "stargate_graphql";
  public static final String TABLE_NAME = "schema_source";
  public static final String KEYSPACE_REPLICATION_PROPERTY =
      "stargate.graphql_first.replication_options";
  private static final Replication DEFAULT_KEYSPACE_REPLICATION = Replication.simpleStrategy(1);
  private static final Replication KEYSPACE_REPLICATION =
      parseReplication(System.getProperty(KEYSPACE_REPLICATION_PROPERTY));

  @VisibleForTesting static final String KEYSPACE_COLUMN_NAME = "keyspace_name";
  @VisibleForTesting static final String VERSION_COLUMN_NAME = "version";
  @VisibleForTesting static final String LATEST_VERSION_COLUMN_NAME = "latest_version";
  @VisibleForTesting static final String CONTENTS_COLUMN_NAME = "contents";
  @VisibleForTesting static final String APPLIED_COLUMN_NAME = "[applied]";

  @VisibleForTesting
  static final String DEPLOYMENT_IN_PROGRESS_COLUMN_NAME = "deployment_in_progress";

  private static final int NUMBER_OF_RETAINED_SCHEMA_VERSIONS = 10;

  private final StargateBridgeClient bridge;

  public SchemaSourceDao(StargateBridgeClient bridge) {
    this.bridge = bridge;
  }

  public List<SchemaSource> getAllVersions(String keyspace) {
    if (!tableExists()) {
      return Collections.emptyList();
    }
    Response response = bridge.executeQuery(schemaQuery(keyspace));
    ResultSet resultSet = response.getResultSet();
    if (resultSet.getRowsCount() == 0) {
      return Collections.emptyList();
    }
    return resultSet.getRowsList().stream()
        .map(r -> toSchemaSource(keyspace, r, resultSet.getColumnsList()))
        .collect(Collectors.toList());
  }

  public Optional<SchemaSource> getSingleVersion(
      String keyspace,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<UUID> maybeVersion) {
    return Futures.getUninterruptibly(getSingleVersionAsync(keyspace, maybeVersion));
  }

  public CompletionStage<Optional<SchemaSource>> getLatestVersionAsync(String keyspace) {
    return getSingleVersionAsync(keyspace, Optional.empty());
  }

  private CompletionStage<Optional<SchemaSource>> getSingleVersionAsync(
      String keyspace,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<UUID> maybeVersion) {
    if (maybeVersion.isPresent() && maybeVersion.get().version() != 1) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    return tableExistsAsync()
        .thenCompose(
            exists -> {
              if (!exists) {
                return CompletableFuture.completedFuture(Optional.empty());
              }
              Query query =
                  maybeVersion
                      .map(version -> schemaQueryWithSpecificVersion(keyspace, version))
                      .orElse(schemaQuery(keyspace));
              return bridge
                  .executeQueryAsync(query)
                  .thenApply(
                      response -> {
                        ResultSet resultSet = response.getResultSet();
                        return resultSet.getRowsCount() == 0
                            ? Optional.empty()
                            : Optional.of(
                                toSchemaSource(
                                    keyspace, resultSet.getRows(0), resultSet.getColumnsList()));
                      });
            });
  }

  private SchemaSource toSchemaSource(String keyspace, Row r, List<ColumnSpec> columns) {
    return new SchemaSource(
        keyspace,
        Rows.getUuid(r, VERSION_COLUMN_NAME, columns),
        Rows.getString(r, CONTENTS_COLUMN_NAME, columns));
  }

  @VisibleForTesting
  Query schemaQueryWithSpecificVersion(String keyspace, UUID uuid) {
    return new QueryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME)
        .column(CONTENTS_COLUMN_NAME)
        .from(KEYSPACE_NAME, TABLE_NAME)
        .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
        .where(VERSION_COLUMN_NAME, Predicate.EQ, Values.of(uuid))
        .build();
  }

  @VisibleForTesting
  Query schemaQuery(String keyspace) {
    return new QueryBuilder()
        .select()
        .column(VERSION_COLUMN_NAME)
        .column(CONTENTS_COLUMN_NAME)
        .from(KEYSPACE_NAME, TABLE_NAME)
        .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
        .orderBy(VERSION_COLUMN_NAME, Column.Order.DESC)
        .build();
  }

  /** @return the new version */
  public SchemaSource insert(String keyspace, String newContents) {

    UUID newVersion = Uuids.timeBased();
    Value newVersionValue = Values.of(newVersion);

    Query insertNewSchema =
        new QueryBuilder()
            .insertInto(KEYSPACE_NAME, TABLE_NAME)
            .value(KEYSPACE_COLUMN_NAME, Values.of(keyspace))
            .value(VERSION_COLUMN_NAME, newVersionValue)
            .value(LATEST_VERSION_COLUMN_NAME, newVersionValue)
            .value(CONTENTS_COLUMN_NAME, Values.of(newContents))
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Values.of(false))
            .build();

    try {
      bridge.executeQuery(insertNewSchema);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Schema deployment for keyspace: %s and version: %s failed.", keyspace, newVersion));
    }
    return new SchemaSource(keyspace, newVersion, newContents);
  }

  private void createTableIfNotExists() {
    bridge.executeQuery(
        new QueryBuilder()
            .create()
            .keyspace(KEYSPACE_NAME)
            .ifNotExists()
            .withReplication(KEYSPACE_REPLICATION)
            .build());
    bridge.executeQuery(
        new QueryBuilder()
            .create()
            .table(KEYSPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .column(
                ImmutableColumn.builder()
                    .name(KEYSPACE_COLUMN_NAME)
                    .kind(Column.Kind.PARTITION_KEY)
                    .type("varchar")
                    .build())
            .column(
                ImmutableColumn.builder()
                    .name(VERSION_COLUMN_NAME)
                    .kind(Column.Kind.CLUSTERING)
                    .type("timeuuid")
                    .order(Column.Order.DESC)
                    .build())
            .column(
                ImmutableColumn.builder()
                    .name(CONTENTS_COLUMN_NAME)
                    .kind(Column.Kind.REGULAR)
                    .type("varchar")
                    .build())
            .column(
                ImmutableColumn.builder()
                    .name(LATEST_VERSION_COLUMN_NAME)
                    .kind(Column.Kind.STATIC)
                    .type("timeuuid")
                    .build())
            .column(
                ImmutableColumn.builder()
                    .name(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME)
                    .kind(Column.Kind.STATIC)
                    .type("boolean")
                    .build())
            .build());
  }

  private CompletionStage<Boolean> tableExistsAsync() {
    return bridge.getTableAsync(KEYSPACE_NAME, TABLE_NAME, false).thenApply(Optional::isPresent);
  }

  private boolean tableExists() {
    return Futures.getUninterruptibly(tableExistsAsync());
  }

  /**
   * "Locks" the table to start a new deployment. Concurrent calls to this method will fail until
   * either {@link #abortDeployment(String)} or {@link #insert(String, String)} have been called.
   *
   * @throws IllegalStateException if the deployment could not be started.
   */
  public void startDeployment(String keyspace, UUID expectedLatestVersion, boolean force) {
    createTableIfNotExists();
    Value versionValue =
        expectedLatestVersion == null ? Values.NULL : Values.of(expectedLatestVersion);
    List<BuiltCondition> conditions =
        force
            ? ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, versionValue))
            : ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, versionValue),
                BuiltCondition.of(
                    DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Predicate.NEQ, Values.of(true)));
    Query updateDeploymentToInProgress =
        new QueryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Values.of(true))
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
            .ifs(conditions)
            .build();

    ResultSet resultSet = bridge.executeQuery(updateDeploymentToInProgress).getResultSet();
    Row row = resultSet.getRows(0);
    List<ColumnSpec> columns = resultSet.getColumnsList();
    if (!Rows.getBoolean(row, APPLIED_COLUMN_NAME, columns)) {
      handleFailedDeployLwt(row, expectedLatestVersion, columns);
    }
  }

  private void handleFailedDeployLwt(
      Row row, UUID expectedLatestVersion, List<ColumnSpec> columns) {
    boolean hasVersion =
        columns.stream().anyMatch(c -> LATEST_VERSION_COLUMN_NAME.equals(c.getName()));
    if (!hasVersion) {
      throw new IllegalStateException(
          "You specified expectedVersion but no previous version was found");
    }
    UUID actualLatestVersion = Rows.getUuid(row, LATEST_VERSION_COLUMN_NAME, columns);
    if (Objects.equals(actualLatestVersion, expectedLatestVersion)) {
      assert Rows.getBoolean(row, DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, columns);
      throw new IllegalStateException(
          "It looks like someone else is deploying a new schema, please check the latest version and try again. "
              + "This can also happen if a previous deployment failed unexpectedly, in that case you can use the "
              + "'force' argument to bypass this check.");
    }
    throw new IllegalStateException(
        String.format(
            "You specified expectedVersion %s, but there is a more recent version %s",
            expectedLatestVersion, actualLatestVersion));
  }

  public void abortDeployment(String keyspace) {
    Query updateDeploymentToNotInProgress =
        new QueryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Values.of(false))
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
            .build();
    bridge.executeQuery(updateDeploymentToNotInProgress);
  }

  public void undeploy(String keyspace, UUID expectedLatestVersion, boolean force) {
    Value versionValue = Values.of(expectedLatestVersion);
    List<BuiltCondition> conditions =
        force
            ? ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, versionValue))
            : ImmutableList.of(
                BuiltCondition.of(LATEST_VERSION_COLUMN_NAME, Predicate.EQ, versionValue),
                BuiltCondition.of(
                    DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Predicate.NEQ, Values.of(true)));
    Query clearLatestVersion =
        new QueryBuilder()
            .update(KEYSPACE_NAME, TABLE_NAME)
            .value(LATEST_VERSION_COLUMN_NAME, Values.NULL)
            .value(DEPLOYMENT_IN_PROGRESS_COLUMN_NAME, Values.of(false))
            .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
            .ifs(conditions)
            .build();
    ResultSet resultSet = bridge.executeQuery(clearLatestVersion).getResultSet();
    Row row = resultSet.getRows(0);
    List<ColumnSpec> columns = resultSet.getColumnsList();
    if (!Rows.getBoolean(row, APPLIED_COLUMN_NAME, columns)) {
      handleFailedDeployLwt(row, expectedLatestVersion, columns);
    }
  }

  public void purgeOldVersions(String keyspace) throws Exception {
    List<SchemaSource> allSchemasForKeyspace = getAllVersions(keyspace);

    int numberOfEntriesToRemove = allSchemasForKeyspace.size() - NUMBER_OF_RETAINED_SCHEMA_VERSIONS;
    if (numberOfEntriesToRemove > 0) {
      LOGGER.info("Removing {} old schema entries.", numberOfEntriesToRemove);

      // remove N oldest entries
      SchemaSource mostRecentToRemove =
          allSchemasForKeyspace.get(NUMBER_OF_RETAINED_SCHEMA_VERSIONS);

      Query deleteSchemaQuery =
          new QueryBuilder()
              .delete()
              .from(KEYSPACE_NAME, TABLE_NAME)
              .where(KEYSPACE_COLUMN_NAME, Predicate.EQ, Values.of(keyspace))
              .where(VERSION_COLUMN_NAME, Predicate.LTE, Values.of(mostRecentToRemove.getVersion()))
              .build();
      bridge.executeQuery(deleteSchemaQuery);
    }
  }

  @VisibleForTesting
  static Replication parseReplication(String spec) {
    if (spec == null) {
      LOGGER.debug("No replication configured, defaulting to {}", DEFAULT_KEYSPACE_REPLICATION);
      return DEFAULT_KEYSPACE_REPLICATION;
    }

    try {
      Replication replication =
          spec.matches("\\d+") ? parseSimpleReplication(spec) : parseNetworkReplication(spec);
      LOGGER.debug("Using configured replication {}", replication);
      return replication;
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Could not parse replication '{}' (from {}). Falling back to default {}",
          spec,
          KEYSPACE_REPLICATION_PROPERTY,
          DEFAULT_KEYSPACE_REPLICATION);
      return DEFAULT_KEYSPACE_REPLICATION;
    }
  }

  private static Replication parseSimpleReplication(String spec) {
    int rf = Integer.parseInt(spec);
    if (rf < 1) {
      throw new IllegalArgumentException();
    }
    return Replication.simpleStrategy(rf);
  }

  private static Replication parseNetworkReplication(String spec) {
    Map<String, String> rawOptions =
        Splitter.on(",").withKeyValueSeparator(Splitter.on("=").trimResults()).split(spec);
    Map<String, Integer> options = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : rawOptions.entrySet()) {
      String dc = entry.getKey();
      if (dc.isEmpty()) {
        throw new IllegalArgumentException();
      }
      int rf = Integer.parseInt(entry.getValue());
      if (rf < 1) {
        throw new IllegalArgumentException();
      }
      options.put(dc, rf);
    }
    return Replication.networkTopologyStrategy(options);
  }
}
