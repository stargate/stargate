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
package io.stargate.web.resources;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import javax.ws.rs.NotFoundException;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Data store abstraction used by Rest API: encapsulates authentication aspects along with
 * underlying actual {@link DataStore}, offers some more convenience access.
 */
public class AuthenticatedDB {

  private final DataStore dataStore;
  private final AuthenticationSubject authenticationSubject;
  private final AuthorizationService authorizationService;

  public AuthenticatedDB(
      DataStore dataStore,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorizationService) {
    this.dataStore = dataStore;
    this.authenticationSubject = authenticationSubject;
    this.authorizationService = authorizationService;
  }

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public AuthenticationSubject getAuthenticationSubject() {
    return authenticationSubject;
  }

  public Collection<Table> getTables(String keyspaceName) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    return keyspace.tables();
  }

  public Table getTable(String keyspaceName, String table) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    Table tableMetadata = keyspace.table(table);
    if (tableMetadata == null) {
      throw new NotFoundException(String.format("table '%s' not found", table));
    }
    return tableMetadata;
  }

  public Set<Keyspace> getKeyspaces() {
    return dataStore.schema().keyspaces();
  }

  public Keyspace getKeyspace(String keyspaceName) {
    return dataStore.schema().keyspace(keyspaceName);
  }

  /**
   * Retrieve user defined types definitions for a keyspace.
   *
   * @param keyspaceName existing keyspace name
   * @return a collection of user defined types definitions
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
   * @param keyspaceName existing keyspace name
   * @param typeName identifier for the type
   * @return a collection of user defined types definitions
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

  public QueryBuilder queryBuilder() {
    return dataStore.queryBuilder();
  }

  public Schema schema() {
    return dataStore.schema();
  }

  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    return dataStore.execute(query, parametersModifier);
  }

  public CompletableFuture<ResultSet> execute(BoundQuery query, ConsistencyLevel consistencyLevel) {
    return dataStore.execute(query, consistencyLevel);
  }
}
