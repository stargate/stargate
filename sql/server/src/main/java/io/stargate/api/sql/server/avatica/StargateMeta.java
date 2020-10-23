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
package io.stargate.api.sql.server.avatica;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.api.sql.server.DataStoreFactory;
import io.stargate.api.sql.server.avatica.StatementHolder.ExecutionResult;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

public class StargateMeta implements Meta {

  private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();

  private final DataStoreFactory dataStoreFactory;
  private final AuthenticationService authenticator;

  public StargateMeta(DataStoreFactory dataStoreFactory, AuthenticationService authenticator) {
    this.dataStoreFactory = dataStoreFactory;
    this.authenticator = authenticator;
  }

  private void newConnection(ConnectionHandle ch, Map<String, String> info) {
    connections.computeIfAbsent(
        ch.id,
        sid -> {
          String username = info.get("user");
          String password = info.get("password");
          if (username == null || password == null) {
            throw new IllegalArgumentException("Missing credentials in connection properties.");
          }

          if (!"token".equals(username)) {
            throw new IllegalArgumentException(
                "Unexpected user name for token authentication: " + username);
          }

          try {
            StoredCredentials credentials = authenticator.validateToken(password);
            username = credentials.getRoleName();
          } catch (UnauthorizedException e) {
            throw new IllegalArgumentException(e);
          }

          DataStore dataStore = dataStoreFactory.create(username);
          return new Connection(dataStore, ch);
        });
  }

  private StatementHolder statement(StatementHandle h) throws NoSuchStatementException {
    return connection(h.connectionId).statement(h);
  }

  private Connection connection(ConnectionHandle ch) {
    return connection(ch.id);
  }

  private Connection connection(String connectionId) {
    return connections.computeIfAbsent(
        connectionId,
        cid -> {
          throw new IllegalArgumentException("Unknown connection id: " + cid);
        });
  }

  @Override
  public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle ch) {
    return Collections.emptyMap();
  }

  @Override
  public MetaResultSet getTables(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getColumns(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getTableTypes(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getProcedures(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getProcedureColumns(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getColumnPrivileges(
      ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getTablePrivileges(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getBestRowIdentifier(
      ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getVersionColumns(
      ConnectionHandle ch, String catalog, String schema, String table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getPrimaryKeys(
      ConnectionHandle ch, String catalog, String schema, String table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getImportedKeys(
      ConnectionHandle ch, String catalog, String schema, String table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getExportedKeys(
      ConnectionHandle ch, String catalog, String schema, String table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getCrossReference(
      ConnectionHandle ch,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getTypeInfo(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getIndexInfo(
      ConnectionHandle ch,
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getUDTs(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getSuperTypes(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getSuperTables(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getAttributes(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getClientInfoProperties(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getFunctions(
      ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getFunctionColumns(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetaResultSet getPseudoColumns(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Object> createIterable(
      StatementHandle stmt,
      QueryState state,
      Signature signature,
      List<TypedValue> parameters,
      Frame firstFrame) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    try {
      Connection connection = connection(ch);
      return connection.newStatement(sql);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public ExecuteResult prepareAndExecute(
      StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteResult prepareAndExecute(
      StatementHandle h,
      String sql,
      long maxRowCount,
      int maxRowsInFirstFrame,
      PrepareCallback callback)
      throws NoSuchStatementException {
    StatementHolder statement = statement(h);
    ExecutionResult result = statement.execute(sql, Collections.emptyList());
    MetaResultSet rs = fetch(result, h, maxRowsInFirstFrame);
    return new ExecuteResult(Collections.singletonList(rs));
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecuteBatchResult executeBatch(
      StatementHandle h, List<List<TypedValue>> parameterValues) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
      throws NoSuchStatementException, MissingResultsException {
    StatementHolder statement = statement(h);
    ExecutionResult result = statement.result();
    if (result == null) {
      throw new MissingResultsException(h);
    }

    return result.fetch(offset, fetchMaxRowCount);
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) {
    throw new UnsupportedOperationException();
  }

  private MetaResultSet fetch(
      StatementHolder.ExecutionResult result, StatementHandle h, int maxRowsInFrame) {
    Signature signature = result.prepared().signature();
    if (result.isUpdate()) {
      long count = result.updateCount();
      return MetaResultSet.count(h.connectionId, h.id, count);
    } else {
      Frame frame = result.fetch(0, maxRowsInFrame);
      return MetaResultSet.create(h.connectionId, h.id, true, signature, frame);
    }
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    StatementHolder s = statement(h);
    ExecutionResult result = s.execute(parameterValues);
    MetaResultSet rs = fetch(result, h, maxRowsInFirstFrame);
    return new ExecuteResult(Collections.singletonList(rs));
  }

  @Override
  public StatementHandle createStatement(ConnectionHandle ch) {
    try {
      Connection connection = connection(ch);
      return connection.newStatement();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void closeStatement(StatementHandle h) {
    connection(h.connectionId).closeStatement(h);
  }

  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    newConnection(ch, info);
  }

  @Override
  public void closeConnection(ConnectionHandle ch) {
    connections.remove(ch.id);
  }

  @Override
  public boolean syncResults(StatementHandle sh, QueryState state, long offset) {
    return false;
  }

  @Override
  public void commit(ConnectionHandle ch) {
    // nop for now
  }

  @Override
  public void rollback(ConnectionHandle ch) {
    // nop for now
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    return null;
  }

  @VisibleForTesting
  Map<String, Connection> connections() {
    return connections;
  }
}
