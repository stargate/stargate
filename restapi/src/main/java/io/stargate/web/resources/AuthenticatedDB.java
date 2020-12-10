package io.stargate.web.resources;

import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import java.util.Collection;
import java.util.Set;
import javax.ws.rs.NotFoundException;

public class AuthenticatedDB {

  private DataStore dataStore;
  private AuthenticationPrincipal authenticationPrincipal;

  public AuthenticatedDB(DataStore dataStore, AuthenticationPrincipal authenticationPrincipal) {
    this.dataStore = dataStore;
    this.authenticationPrincipal = authenticationPrincipal;
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

  public DataStore getDataStore() {
    return dataStore;
  }

  public void setDataStore(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public AuthenticationPrincipal getAuthenticationPrincipal() {
    return authenticationPrincipal;
  }

  public void setAuthenticationPrincipal(AuthenticationPrincipal authenticationPrincipal) {
    this.authenticationPrincipal = authenticationPrincipal;
  }
}
