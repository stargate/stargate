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
package io.stargate.graphql.core;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.Table;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeyspaceFetcher {
  public Persistence persistence;
  private AuthenticationService authenticationService;

  public KeyspaceFetcher(Persistence persistence, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
  }

  public class KeyspaceByNameFetcher implements DataFetcher {

    @Override
    public Object get(DataFetchingEnvironment environment) throws Exception {
      HTTPAwareContextImpl httpAwareContext = environment.getContext();

      String token = httpAwareContext.getAuthToken();
      StoredCredentials storedCredentials = authenticationService.validateToken(token);
      ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
      QueryState queryState = persistence.newQueryState(clientState);
      DataStore dataStore = persistence.newDataStore(queryState, null);

      String keyspaceName = environment.getArgument("name");
      Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
      if (keyspace == null) {
        return null;
      }
      return formatResult(keyspace, environment);
    }
  }

  public class KeyspacesFetcher implements DataFetcher {
    @Override
    public Object get(DataFetchingEnvironment environment) throws Exception {
      HTTPAwareContextImpl httpAwareContext = environment.getContext();

      String token = httpAwareContext.getAuthToken();
      StoredCredentials storedCredentials = authenticationService.validateToken(token);
      ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
      QueryState queryState = persistence.newQueryState(clientState);
      DataStore dataStore = persistence.newDataStore(queryState, null);
      return formatResult(dataStore.schema().keyspaces(), environment);
    }
  }

  private List formatResult(Set<Keyspace> keyspaces, DataFetchingEnvironment environment) {
    List list = new ArrayList();
    for (Keyspace keyspace : keyspaces) {
      list.add(formatResult(keyspace, environment));
    }
    return list;
  }

  private Map<String, Object> formatResult(Keyspace keyspace, DataFetchingEnvironment environment) {
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("name", keyspace.name());
    if (environment.getSelectionSet().getField("tables") != null) {
      builder.put("tables", buildTables(keyspace.tables()));
    }

    SelectedField tableField;
    if ((tableField = environment.getSelectionSet().getField("table")) != null) {
      String tableName = (String) tableField.getArguments().get("name");
      Table table = keyspace.table(tableName);
      if (table != null) {
        builder.put("table", buildTable(table));
      }
    }
    builder.put("dcs", buildDcs(keyspace));
    return builder.build();
  }

  private List buildDcs(Keyspace keyspace) {
    List list = new ArrayList();
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

  private List buildTables(Set<Table> tables) {
    List list = new ArrayList();
    for (Table table : tables) {
      list.add(buildTable(table));
    }
    return list;
  }

  private Map buildTable(Table table) {
    return ImmutableMap.of(
        "name", table.name(),
        "columns", buildColumns(table.columns()));
  }

  private List buildColumns(List<Column> columns) {
    List list = new ArrayList();
    for (Column column : columns) {
      list.add(buildColumn(column));
    }
    return list;
  }

  private Map buildColumn(Column column) {
    return ImmutableMap.of(
        "kind", buildColumnKind(column),
        "name", column.name(),
        "type", buildDataType(column.type()));
  }

  private Object buildDataType(Column.ColumnType columntype) {
    if (columntype.isParameterized()) {
      return ImmutableMap.of(
          "basic", buildBasicType(columntype),
          "info", buildDataTypeInfo(columntype));
    }

    return ImmutableMap.of("basic", buildBasicType(columntype));
  }

  private Object buildDataTypeInfo(Column.ColumnType columntype) {
    if (columntype.isParameterized()) {
      List list = new ArrayList();
      for (Column.ColumnType type : columntype.parameters()) {
        list.add(buildDataType(type));
      }
      return ImmutableMap.of("subTypes", list);
    }
    return null;
  }

  private Object buildBasicType(Column.ColumnType columntype) {
    return columntype.rawType().name().toUpperCase();
  }

  private Object buildColumnKind(Column column) {
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
