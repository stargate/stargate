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
package io.stargate.web.restapi.dao;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import javax.ws.rs.NotFoundException;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Data access abstraction used by Rest API: encapsulates authentication aspects as well as access
 * to underlying actual {@link DataStore}.
 */
public class RestDB {
  private final DataStore dataStore;
  private final AuthenticationSubject authenticationSubject;
  private final AuthorizationService authorizationService;

  public RestDB(
      DataStore dataStore,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorizationService) {
    this.dataStore = dataStore;
    this.authenticationSubject = authenticationSubject;
    this.authorizationService = authorizationService;
  }

  /**
   * Method for trying to find and return all tables for given keyspace. Keyspace must exist for
   * call to work; otherise {@link NotFoundException} will be thrown
   *
   * @param keyspaceName Name of keyspace to look for tables (must exist)
   * @return A collection that contains all tables for given keyspace
   * @throws NotFoundException If no keyspace with given keyspace exists in the underlying data
   *     store
   */
  public Collection<Table> getTables(String keyspaceName) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    return keyspace.tables();
  }

  /**
   * Method for trying to find specific table or materialized view that exists in given keyspace.
   * Keyspace must exist for call to work; otherwise {@link NotFoundException} will be thrown
   *
   * @param keyspaceName Name of keyspace to look for tables (must exist)
   * @param table Name of table or materialized view to look for (must exist)
   * @return Metadata for Table requested
   * @throws NotFoundException If no keyspace with given name exists in the underlying data store,
   *     or if no table with specified name exists within that keyspace.
   */
  public AbstractTable getTable(String keyspaceName, String table) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    // We could be working with a table or materialized view here but we don't know which so try by
    // table and then fallback to materialized view.
    AbstractTable tableMetadata = keyspace.table(table);
    if (tableMetadata == null) {
      tableMetadata = keyspace.materializedView(table);
      if (tableMetadata == null) {
        throw new NotFoundException(String.format("table '%s' not found", table));
      }
    }
    return tableMetadata;
  }

  /**
   * Method for finding and returning metadata for all keyspaces for the underlying data store.
   *
   * @return A set of metadata for all keyspaces the underlying data store has.
   */
  public Set<Keyspace> getKeyspaces() {
    return dataStore.schema().keyspaces();
  }

  /**
   * Method for trying to find and return metadata for given keyspace, if one exists; if none,
   * {@code null} is returned.
   *
   * @param keyspaceName Name of keyspace to look for
   * @return Metadata for keyspace requested if one exists; {@code null} otherwise.
   */
  public Keyspace getKeyspace(String keyspaceName) {
    return dataStore.schema().keyspace(keyspaceName);
  }

  /**
   * Retrieve user defined types definitions for a keyspace. Keyspace must exist, otherwise a {@link
   * NotFoundException} is thrown.
   *
   * @param keyspaceName existing keyspace name
   * @return a collection of user defined types definitions within specified keyspace
   * @throws NotFoundException If no keyspace with given keyspace exists in the underlying data
   *     store
   */
  public Collection<UserDefinedType> getTypes(String keyspaceName) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }
    return keyspace.userDefinedTypes();
  }

  /**
   * Retrieve user defined types definitions from its identifier in a keyspace.
   *
   * @param keyspaceName existing keyspace name (must exist)
   * @param typeName identifier for the type (must exist)
   * @return Metadat for the user defined type requested
   * @throws NotFoundException If no keyspace with given keyspace exists in the underlying data
   *     store, or if no user defined type with given name exists in that keyspace.
   */
  public UserDefinedType getType(String keyspaceName, String typeName) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    UserDefinedType typeMetadata = keyspace.userDefinedType(typeName);
    if (typeMetadata == null) {
      throw new NotFoundException(
          String.format("type '%s' not found in the keyspace '%s'", typeName, keyspaceName));
    }
    return typeMetadata;
  }

  /**
   * Accessor for getting a new {@link QueryBuilder} to use for constructing queries against
   * underlying data store
   *
   * @return New {@link QueryBuilder} instance
   */
  public QueryBuilder queryBuilder() {
    return dataStore.queryBuilder();
  }

  public Schema schema() {
    return dataStore.schema();
  }

  public void authorizeDataRead(String keyspace, String table, SourceAPI sourceAPI)
      throws UnauthorizedException {
    authorizationService.authorizeDataRead(authenticationSubject, keyspace, table, sourceAPI);
  }

  public ResultSet authorizedDataRead(
      Callable<ResultSet> action,
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      SourceAPI sourceAPI)
      throws Exception {
    return authorizationService.authorizedDataRead(
        action, authenticationSubject, keyspace, table, typedKeyValues, sourceAPI);
  }

  public void authorizeDataWrite(
      String keyspace,
      String table,
      List<TypedKeyValue> typedKeyValues,
      Scope scope,
      SourceAPI sourceAPI)
      throws UnauthorizedException {
    authorizationService.authorizeDataWrite(
        authenticationSubject, keyspace, table, typedKeyValues, scope, sourceAPI);
  }

  public void authorizeSchemaRead(
      List<String> keyspaceNames,
      List<String> tableNames,
      SourceAPI sourceAPI,
      ResourceKind resource)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaRead(
        authenticationSubject, keyspaceNames, tableNames, sourceAPI, resource);
  }

  public void authorizeSchemaWrite(
      String keyspace, String table, Scope scope, SourceAPI sourceAPI, ResourceKind resource)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaWrite(
        authenticationSubject, keyspace, table, scope, sourceAPI, resource);
  }

  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    return dataStore.execute(query, parametersModifier);
  }

  public CompletableFuture<ResultSet> execute(BoundQuery query, ConsistencyLevel consistencyLevel) {
    return dataStore.execute(query, consistencyLevel);
  }
}
