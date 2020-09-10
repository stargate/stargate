package io.stargate.web.resources;

import javax.ws.rs.NotFoundException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.ClientState;
import io.stargate.db.DefaultQueryOptions;
import io.stargate.db.Persistence;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.Table;

public class Db {
  private final Persistence persistence;
  private final DataStore dataStore;
  private final AuthenticationService authenticationService;

  public Collection<Table> getTables(DataStore dataStore, String keyspaceName) {
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    return keyspace.tables();
  }

  public Table getTable(DataStore dataStore, String keyspaceName, String table) {
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      throw new NotFoundException(String.format("keyspace '%s' not found", keyspaceName));
    }

    Table tableMetadata = keyspace.table(table);
    if (tableMetadata == null) {
      throw new NotFoundException(String.format("table '%s' not found", table));
    }
    return tableMetadata;
  }

  public Db(final Persistence<?, ?, ?> persistence, AuthenticationService authenticationService) {
    this.authenticationService = authenticationService;
    this.persistence = persistence;
    ClientState clientState = persistence.newClientState("");
    QueryState queryState = persistence.newQueryState(clientState);
    this.dataStore = persistence.newDataStore(queryState, null);
  }

  public DataStore getDataStore() {
    return this.dataStore;
  }

  public Persistence<?, ?, ?> getPersistence() {
    return this.persistence;
  }

  public DataStore getDataStoreForToken(String token) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = this.persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = this.persistence.newQueryState(clientState);

    return this.persistence.newDataStore(queryState, null);
  }

  public DataStore getDataStoreForToken(String token, int pageSize, ByteBuffer pagingState) throws UnauthorizedException {
    StoredCredentials storedCredentials = authenticationService.validateToken(token);
    ClientState clientState = this.persistence.newClientState(storedCredentials.getRoleName());
    QueryState queryState = this.persistence.newQueryState(clientState);

    QueryOptions queryOptions = DefaultQueryOptions.builder()
                                                   .options(DefaultQueryOptions.SpecificOptions.builder()
                                                                               .pageSize(pageSize)
                                                                               .pagingState(pagingState)
                                                                               .build()
                                                   )
                                                   .build();


    return this.persistence.newDataStore(queryState, queryOptions);
  }
}
