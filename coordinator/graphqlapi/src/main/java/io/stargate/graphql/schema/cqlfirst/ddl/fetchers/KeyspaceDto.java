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
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyspaceDto {

  private static final Logger LOG = LoggerFactory.getLogger(KeyspaceDto.class);

  private final Keyspace keyspace;
  private final AuthorizationService authorizationService;
  private final AuthenticationSubject authenticationSubject;

  public KeyspaceDto(
      Keyspace keyspace,
      AuthorizationService authorizationService,
      AuthenticationSubject authenticationSubject) {

    this.keyspace = keyspace;
    this.authorizationService = authorizationService;
    this.authenticationSubject = authenticationSubject;
  }

  public String getName() {
    return keyspace.name();
  }

  public List<Map<String, String>> getDcs() {
    List<Map<String, String>> list = new ArrayList<>();
    for (Map.Entry<String, String> entries : keyspace.replication().entrySet()) {
      if (entries.getKey().equals("class")) continue;
      if (entries.getKey().equals("replication_factor")) continue;
      list.add(
          ImmutableMap.of(
              "name", entries.getKey(),
              "replicas", entries.getValue()));
    }

    return list;
  }

  public List<Map<String, Object>> getTables() {
    return keyspace.tables().stream()
        .filter(this::isAuthorized)
        .map(KeyspaceDto::buildTable)
        .collect(Collectors.toList());
  }

  public Map<String, Object> getTable(DataFetchingEnvironment environment) {
    String name = environment.getArgument("name");
    Table table = keyspace.table(name);
    return (table == null || !isAuthorized(table)) ? null : buildTable(table);
  }

  private boolean isAuthorized(Table table) {
    try {
      authorizationService.authorizeSchemaRead(
          authenticationSubject,
          Collections.singletonList(keyspace.name()),
          Collections.singletonList(table.name()),
          SourceAPI.GRAPHQL,
          ResourceKind.TABLE);
      return true;
    } catch (UnauthorizedException e) {
      LOG.debug(
          "Not returning table {}.{} due to not being authorized", keyspace.name(), table.name());
      return false;
    }
  }

  public List<Map<String, Object>> getTypes() {
    return keyspace.userDefinedTypes().stream()
        .map(KeyspaceDto::buildUdt)
        .collect(Collectors.toList());
  }

  public Map<String, Object> getType(DataFetchingEnvironment environment) {
    String name = environment.getArgument("name");
    UserDefinedType type = keyspace.userDefinedType(name);
    return type == null ? null : buildUdt(type);
  }

  private static Map<String, Object> buildTable(Table table) {
    return ImmutableMap.of(
        "name", table.name(),
        "columns", buildColumns(table.columns(), true));
  }

  private static Map<String, Object> buildUdt(UserDefinedType type) {
    return ImmutableMap.of(
        "name", type.name(),
        "fields", buildColumns(type.columns(), false));
  }

  private static List<Map<String, Object>> buildColumns(List<Column> columns, boolean includeKind) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column column : columns) {
      list.add(buildColumn(column, includeKind));
    }
    return list;
  }

  private static Map<String, Object> buildColumn(Column column, boolean includeKind) {
    return includeKind
        ? ImmutableMap.of(
            "kind", buildColumnKind(column),
            "name", column.name(),
            "type", buildDataType(column.type()))
        : ImmutableMap.of(
            "name", column.name(),
            "type", buildDataType(column.type()));
  }

  private static Map<String, Object> buildDataType(Column.ColumnType columnType) {
    if (columnType.isUserDefined()) {
      return ImmutableMap.of(
          "basic",
          buildBasicType(columnType),
          "info",
          ImmutableMap.of("name", columnType.name(), "frozen", columnType.isFrozen()));
    } else if (columnType.isCollection() || columnType.isTuple()) {
      return ImmutableMap.of(
          "basic", buildBasicType(columnType),
          "info", buildParameterizedDataTypeInfo(columnType));
    } else {
      return ImmutableMap.of("basic", buildBasicType(columnType));
    }
  }

  private static Map<String, Object> buildParameterizedDataTypeInfo(Column.ColumnType columnType) {
    assert columnType.isParameterized();
    List<Map<String, Object>> list = new ArrayList<>();
    for (Column.ColumnType type : columnType.parameters()) {
      list.add(buildDataType(type));
    }
    return ImmutableMap.of("subTypes", list, "frozen", columnType.isFrozen());
  }

  private static String buildBasicType(Column.ColumnType columnType) {
    return columnType.rawType().name().toUpperCase();
  }

  private static String buildColumnKind(Column column) {
    switch (column.kind()) {
      case PartitionKey:
        return "PARTITION";
      case Clustering:
        return "CLUSTERING";
      case Regular:
        return "REGULAR";
      case Static:
        return "STATIC";
    }
    return "UNKNOWN";
  }
}
