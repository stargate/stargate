package io.stargate.graphql.schema.schemafirst.fetchers.admin;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.*;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import java.time.ZonedDateTime;
import java.util.*;

public abstract class SchemaFetcher<ResultT> extends CassandraFetcher<ResultT> {
  public SchemaFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  protected Map<String, Object> mapToSource(String namespace, SchemaSource source) {
    ImmutableMap.Builder<String, Object> result = ImmutableMap.builder();
    result.put("namespace", namespace);
    if (source != null) {
      UUID version = source.getVersion();
      ZonedDateTime deployDate = source.getDeployDate();
      String contents =
          String.format("# Schema for '%s', version %s (%s)\n\n", namespace, version, deployDate)
              + source.getContents();
      result.put("version", version);
      result.put("deployDate", deployDate);
      result.put("contents", contents);
    }
    return result.build();
  }

  protected void authorize(AuthenticationSubject authenticationSubject, String namespace)
      throws UnauthorizedException {
    authorizationService.authorizeSchemaRead(
        authenticationSubject,
        Collections.singletonList(namespace),
        Collections.singletonList(SchemaSourceDao.TABLE_NAME),
        SourceAPI.GRAPHQL);
  }

  protected String getNamespace(DataFetchingEnvironment environment, DataStore dataStore) {
    String namespace = environment.getArgument("namespace");
    if (!dataStore.schema().keyspaceNames().contains(namespace)) {
      throw new IllegalArgumentException(
          String.format("Namespace '%s' does not exist.", namespace));
    }
    return namespace;
  }
}
