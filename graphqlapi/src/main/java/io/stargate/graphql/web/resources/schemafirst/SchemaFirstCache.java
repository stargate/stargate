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
package io.stargate.graphql.web.resources.schemafirst;

import com.google.common.base.Throwables;
import graphql.GraphQL;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.schemafirst.SchemaSource;
import io.stargate.graphql.persistence.schemafirst.SchemaSourceDao;
import io.stargate.graphql.schema.schemafirst.AdminSchemaBuilder;
import io.stargate.graphql.schema.schemafirst.migration.CassandraMigrator;
import io.stargate.graphql.schema.schemafirst.processor.ProcessedSchema;
import io.stargate.graphql.schema.schemafirst.processor.SchemaProcessor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaFirstCache {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaFirstCache.class);

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;
  private final SchemaSourceDao schemaSourceDao;

  private final GraphQL adminGraphql;
  private final ConcurrentMap<String, GraphqlHolder> graphqlHolders;

  public SchemaFirstCache(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory,
      SchemaSourceDao schemaSourceDao) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;
    this.schemaSourceDao = schemaSourceDao;

    this.adminGraphql =
        GraphQL.newGraphQL(
                new AdminSchemaBuilder(
                        authenticationService, authorizationService, dataStoreFactory)
                    .build())
            .build();

    this.graphqlHolders = initSchemas();
  }

  public GraphQL getAdminGraphql() {
    return adminGraphql;
  }

  public GraphQL getGraphql(String namespace, Map<String, String> headers) throws Exception {
    return getGraphql(persistence.decorateKeyspaceName(namespace, headers));
  }

  private GraphQL getGraphql(String namespace) throws Exception {
    SchemaSource latestSource = schemaSourceDao.getLatest(namespace);
    if (latestSource == null) {
      if (graphqlHolders.remove(namespace) != null) {
        LOG.debug(
            "Removing cached schema for {} because keyspace does not exist, "
                + "or schema table does not exist or is empty",
            namespace);
      }
      return null;
    }

    GraphqlHolder currentHolder = graphqlHolders.get(namespace);
    if (currentHolder != null
        && currentHolder.getSource().getVersion().equals(latestSource.getVersion())) {
      LOG.trace("Returning cached schema for {}", namespace);
      return currentHolder.getGraphql();
    }

    GraphqlHolder newHolder =
        new GraphqlHolder(latestSource, persistence.schema().keyspace(namespace));
    boolean installed =
        (currentHolder == null)
            ? graphqlHolders.putIfAbsent(namespace, newHolder) == null
            : graphqlHolders.replace(namespace, currentHolder, newHolder);

    if (installed) {
      LOG.debug("Installing new version for {} in the cache", namespace);
      newHolder.init();
      return newHolder.getGraphql();
    }

    LOG.debug("Got beat installing new version for {} in the cache, fetching again", namespace);
    currentHolder = graphqlHolders.get(namespace);
    return currentHolder == null ? null : currentHolder.getGraphql();
  }

  private ConcurrentMap<String, GraphqlHolder> initSchemas() {
    ConcurrentHashMap<String, GraphqlHolder> result = new ConcurrentHashMap<>();
    for (Keyspace keyspace : persistence.schema().keyspaces()) {
      String namespace = keyspace.name();
      SchemaSource source;
      try {
        source = schemaSourceDao.getLatest(namespace);
      } catch (Exception e) {
        // The only way this can happen is if the schema_source table exists but with the wrong
        // schema.
        LOG.debug(
            "Error while loading persisted schema for {} ({}).  Skipping for now, this will be "
                + "surfaced again if someone tries to execute a GraphQL query in that namespace.",
            namespace,
            e.getMessage());
        continue;
      }
      if (source != null) {
        GraphqlHolder holder = new GraphqlHolder(source, keyspace);
        holder.init();
        result.put(namespace, holder);
      }
    }
    return result;
  }

  class GraphqlHolder {
    private final SchemaSource source;
    private final Keyspace keyspace;
    private final CompletableFuture<GraphQL> graphqlFuture = new CompletableFuture<>();

    GraphqlHolder(SchemaSource source, Keyspace keyspace) {
      this.source = source;
      this.keyspace = keyspace;
    }

    void init() {
      try {
        graphqlFuture.complete(buildGraphql());
      } catch (Exception e) {
        graphqlFuture.completeExceptionally(e);
      }
    }

    private GraphQL buildGraphql() {
      ProcessedSchema processedSchema =
          new SchemaProcessor(authenticationService, authorizationService, dataStoreFactory, true)
              .process(source.getContents(), keyspace);
      // Check that the data model still matches
      CassandraMigrator.forPersisted().compute(processedSchema.getMappingModel(), keyspace);

      return processedSchema.getGraphql();
    }

    public SchemaSource getSource() {
      return source;
    }

    GraphQL getGraphql() throws Exception {
      try {
        return graphqlFuture.get();
      } catch (ExecutionException e) {
        Throwables.throwIfUnchecked(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }
  }
}
