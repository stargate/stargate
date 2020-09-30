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
package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.datastore.InternalDataStore.EXECUTOR;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import io.stargate.db.Authenticator;
import io.stargate.db.BatchType;
import io.stargate.db.ClientState;
import io.stargate.db.EventListener;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.cassandra.datastore.InternalDataStore;
import io.stargate.db.cassandra.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.common.AbstractCassandraPersistence;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPersistence
    extends AbstractCassandraPersistence<
        Config,
        org.apache.cassandra.service.ClientState,
        org.apache.cassandra.service.QueryState,
        KeyspaceMetadata,
        CFMetaData,
        ColumnDefinition,
        UserType,
        IndexMetadata,
        ViewDefinition> {
  private static final Logger logger = LoggerFactory.getLogger(CassandraPersistence.class);

  /*
   * Initial schema migration can take greater than 2 * MigrationManager.MIGRATION_DELAY_IN_MS if a
   * live token owner doesn't become live within MigrationManager.MIGRATION_DELAY_IN_MS. Because it's
   * unknown how long a schema migration takes this waits for an extra MIGRATION_DELAY_IN_MS.
   */
  private static final int STARTUP_DELAY_MS =
      Integer.getInteger("stargate.startup_delay_ms", 3 * MigrationManager.MIGRATION_DELAY_IN_MS);

  private CassandraDaemon daemon;
  private Authenticator authenticator;
  private QueryHandler handler;
  private QueryInterceptor interceptor;

  // C* listener that ensures that our Stargate schema remains up-to-date with the internal C* one.
  private MigrationListener migrationListener;

  public CassandraPersistence() {
    super("Apache Cassandra");
  }

  @Override
  protected SchemaConverter schemaConverter() {
    return new SchemaConverter();
  }

  @Override
  protected Iterable<KeyspaceMetadata> currentInternalSchema() {
    return Iterables.transform(org.apache.cassandra.db.Keyspace.all(), Keyspace::getMetadata);
  }

  @Override
  protected void registerInternalSchemaListener(Runnable runOnSchemaChange) {
    migrationListener =
        new SimpleCallbackMigrationListener() {
          @Override
          void onSchemaChange() {
            runOnSchemaChange.run();
          }
        };
    MigrationManager.instance.register(migrationListener);
  }

  @Override
  protected void unregisterInternalSchemaListener() {
    if (migrationListener != null) {
      MigrationManager.instance.unregister(migrationListener);
    }
  }

  @Override
  protected void initializePersistence(Config config) {
    daemon = new CassandraDaemon(true);

    DatabaseDescriptor.daemonInitialization(() -> config);
    try {
      daemon.init(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable to start Cassandra persistence layer", e);
    }

    // Use special gossip state "X10" to differentiate stargate nodes
    Gossiper.instance.addLocalApplicationState(
        ApplicationState.X10, StorageService.instance.valueFactory.releaseVersion("stargate"));

    daemon.start();

    waitForSchema(STARTUP_DELAY_MS);

    authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
    handler = org.apache.cassandra.service.ClientState.getCQLQueryHandler();
    interceptor = new DefaultQueryInterceptor();

    interceptor.initialize();
  }

  @Override
  protected void destroyPersistence() {
    if (daemon != null) {
      daemon.deactivate();
      daemon = null;
    }
  }

  @Override
  public void registerEventListener(EventListener listener) {
    EventListenerWrapper wrapper = new EventListenerWrapper(listener);
    MigrationManager.instance.register(wrapper);
    interceptor.register(wrapper);
  }

  @Override
  public boolean isRpcReady(InetAddressAndPort endpoint) {
    return StorageService.instance.isRpcReady(Conversion.toInternal(endpoint));
  }

  @Override
  public InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint) {
    try {
      return InetAddressAndPort.getByName(
          StorageService.instance.getRpcaddress(Conversion.toInternal(endpoint)));
    } catch (UnknownHostException e) {
      // That should not happen, so log an error, but return the
      // endpoint address since there's a good change this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      return InetAddressAndPort.getByAddressOverrideDefaults(
          endpoint.address, DatabaseDescriptor.getNativeTransportPort());
    }
  }

  @Override
  public DataStore newDataStore(
      QueryState<org.apache.cassandra.service.QueryState> state, QueryOptions queryOptions) {
    return new InternalDataStore(
        this, Conversion.toInternal(state), Conversion.toInternal(queryOptions));
  }

  @Override
  public QueryState newQueryState(ClientState clientState) {
    return new QueryStateWrapper(clientState);
  }

  @Override
  public ClientState<org.apache.cassandra.service.ClientState> newClientState(
      SocketAddress remoteAddress, InetSocketAddress publicAddress) {
    if (remoteAddress == null) {
      throw new IllegalArgumentException("No remote address provided");
    }

    if (authenticator.requireAuthentication()) {
      return ClientStateWrapper.forExternalCalls(remoteAddress, publicAddress);
    }

    assert remoteAddress instanceof InetSocketAddress;
    ClientStateWrapper state =
        ClientStateWrapper.forExternalCalls((InetSocketAddress) remoteAddress, publicAddress);
    state.login(
        new AuthenticatorWrapper.AuthenticatedUserWrapper(AuthenticatedUser.ANONYMOUS_USER));
    return state;
  }

  @Override
  public ClientState newClientState(String name) {
    if (Strings.isNullOrEmpty(name)) {
      return ClientStateWrapper.forInternalCalls();
    }

    ClientStateWrapper state = ClientStateWrapper.forExternalCalls(null);
    state.login(new AuthenticatorWrapper.AuthenticatedUserWrapper(new AuthenticatedUser(name)));
    return state;
  }

  @Override
  public io.stargate.db.AuthenticatedUser<?> newAuthenticatedUser(String name) {
    return new AuthenticatorWrapper.AuthenticatedUserWrapper(new AuthenticatedUser(name));
  }

  @Override
  public Authenticator getAuthenticator() {
    return authenticator;
  }

  @Override
  public CompletableFuture<? extends Result> query(
      String cql,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      boolean isTracingRequested,
      long queryStartNanoTime) {
    CompletableFuture<Result> future = new CompletableFuture<>();

    EXECUTOR.submit(
        () -> {
          try {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

            UUID tracingId = null;
            if (isTracingRequested) {
              tracingId = UUIDGen.getTimeUUID();
              internalState.prepareTracingSession(tracingId);
            }

            if (internalState.traceNextQuery()) {
              internalState.createTracingSession(customPayload);
              beginTraceQuery(cql, state, options);
            }

            ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(cql, internalState);
            internalOptions.prepare(prepared.boundNames);
            CQLStatement statement = prepared.statement;

            Result result =
                interceptor.interceptQuery(
                    handler, statement, state, options, customPayload, queryStartNanoTime);
            if (result == null) {
              result =
                  Conversion.toResult(
                      QueryProcessor.instance.processStatement(
                          statement, internalState, internalOptions, queryStartNanoTime),
                      internalOptions.getProtocolVersion());
            }

            future.complete(result.setTracingId(tracingId));
          } catch (Throwable t) {
            Conversion.handleException(future, t);
          } finally {
            Tracing.instance.stopSession();
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<? extends Result> execute(
      MD5Digest id,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      boolean isTracingRequested,
      long queryStartNanoTime) {
    CompletableFuture<Result> future = new CompletableFuture<>();

    EXECUTOR.submit(
        () -> {
          try {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

            UUID tracingId = null;
            if (isTracingRequested) {
              tracingId = UUIDGen.getTimeUUID();
              internalState.prepareTracingSession(tracingId);
            }

            ParsedStatement.Prepared prepared = handler.getPrepared(Conversion.toInternal(id));

            if (prepared == null) {
              throw new PreparedQueryNotFoundException(id);
            }

            // Please note that this needs to happen _before_ the beginTraceExecute, because when
            // we add bound values to the trace, we rely on the values having been re-ordered by
            // the following prepare (if named values were used that is).
            internalOptions.prepare(prepared.boundNames);

            if (internalState.traceNextQuery()) {
              internalState.createTracingSession(customPayload);
              beginTraceExecute(
                  prepared, state, internalOptions, internalOptions.getProtocolVersion());
            }

            CQLStatement statement = prepared.statement;

            Result result =
                interceptor.interceptQuery(
                    handler, statement, state, options, customPayload, queryStartNanoTime);
            if (result == null) {
              result =
                  Conversion.toResult(
                      QueryProcessor.instance.processPrepared(
                          statement, internalState, internalOptions, queryStartNanoTime),
                      internalOptions.getProtocolVersion());
            }

            future.complete(result.setTracingId(tracingId));
          } catch (Throwable t) {
            Conversion.handleException(future, t);
          } finally {
            Tracing.instance.stopSession();
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<? extends Result> prepare(
      String cql,
      QueryState state,
      Map<String, ByteBuffer> customPayload,
      boolean isTracingRequested) {
    CompletableFuture<Result> future = new CompletableFuture<>();

    InternalDataStore.EXECUTOR.submit(
        () -> {
          try {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

            UUID tracingId = null;
            if (isTracingRequested) {
              tracingId = UUIDGen.getTimeUUID();
              internalState.prepareTracingSession(tracingId);
            }

            if (internalState.traceNextQuery()) {
              internalState.createTracingSession(customPayload);
              beginTracePrepare(cql, state);
            }

            future.complete(
                Conversion.toPrepared(handler.prepare(cql, internalState, customPayload))
                    .setTracingId(tracingId));
          } catch (Throwable t) {
            Conversion.handleException(future, t);
          } finally {
            Tracing.instance.stopSession();
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<? extends Result> batch(
      BatchType type,
      List<Object> queryOrIds,
      List<List<ByteBuffer>> values,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      boolean isTracingRequested,
      long queryStartNanoTime) {
    CompletableFuture<Result> future = new CompletableFuture<>();

    EXECUTOR.submit(
        () -> {
          try {
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

            UUID tracingId = null;
            if (isTracingRequested) {
              tracingId = UUIDGen.getTimeUUID();
              internalState.prepareTracingSession(tracingId);
            }

            if (internalState.traceNextQuery()) {
              internalState.createTracingSession(customPayload);
              beginTraceBatch(state, options);
            }

            List<Object> queryOrIdList = Conversion.toInternalQueryOrIds(queryOrIds);
            BatchStatement.Type internalBatchType = Conversion.toInternal(type);

            int boundTerms = 0;
            List<ParsedStatement.Prepared> prepared = new ArrayList<>(queryOrIdList.size());
            for (int i = 0; i < queryOrIdList.size(); i++) {
              Object query = queryOrIdList.get(i);
              ParsedStatement.Prepared p;
              if (query instanceof String) {
                org.apache.cassandra.service.ClientState cs = internalState.getClientState();
                if (options.getKeyspace() != null) {
                  cs.setKeyspace(options.getKeyspace());
                }

                p = QueryProcessor.parseStatement((String) query, internalState);
              } else {
                p = handler.getPrepared((org.apache.cassandra.utils.MD5Digest) query);
                if (null == p) {
                  throw new PreparedQueryNotFoundException((MD5Digest) query);
                }
              }

              List<ByteBuffer> queryValues = values.get(i);
              if (queryValues.size() != p.statement.getBoundTerms()) {
                throw new InvalidRequestException(
                    String.format(
                        "There were %d markers(?) in CQL but %d bound variables",
                        p.statement.getBoundTerms(), queryValues.size()));
              }
              boundTerms = p.statement.getBoundTerms();
              prepared.add(p);
            }

            BatchQueryOptions batchOptions =
                BatchQueryOptions.withPerStatementVariables(internalOptions, values, queryOrIdList);
            List<ModificationStatement> statements = new ArrayList<>(prepared.size());
            for (int i = 0; i < prepared.size(); i++) {
              ParsedStatement.Prepared p = prepared.get(i);
              batchOptions.prepareStatement(i, p.boundNames);

              if (!(p.statement instanceof ModificationStatement)) {
                throw new InvalidRequestException(
                    "Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
              }

              statements.add((ModificationStatement) p.statement);
            }

            // Note: It's ok at this point to pass a bogus value for the number of bound terms in
            // the BatchState ctor
            // (and no value would be really correct, so we prefer passing a clearly wrong one).
            BatchStatement batch =
                new BatchStatement(boundTerms, internalBatchType, statements, Attributes.none());

            ResultMessage response =
                handler.processBatch(
                    batch, internalState, batchOptions, customPayload, queryStartNanoTime);

            future.complete(
                Conversion.toResult(response, internalOptions.getProtocolVersion())
                    .setTracingId(tracingId));
          } catch (Exception e) {
            JVMStabilityInspector.inspectThrowable(e);
            Conversion.handleException(future, e);
          } finally {
            Tracing.instance.stopSession();
          }
        });

    return future;
  }

  @Override
  public boolean isInSchemaAgreement() {
    // We only include live nodes because this method is mainly used to wait for schema
    // agreement, and waiting for failed nodes is not a great idea.
    // Also note that in theory getSchemaVersion can return null for some nodes, and if it does
    // the code below will likely return false (the null will be an element on its own), but that's
    // probably the right answer in that case. In practice, this shouldn't be a problem though.
    return Gossiper.instance.getLiveTokenOwners().stream()
            .map(Gossiper.instance::getSchemaVersion)
            .collect(Collectors.toSet())
            .size()
        <= 1;
  }

  @Override
  public void captureClientWarnings() {
    ClientWarn.instance.captureWarnings();
  }

  @Override
  public List<String> getClientWarnings() {
    return ClientWarn.instance.getWarnings();
  }

  @Override
  public void resetClientWarnings() {
    ClientWarn.instance.resetWarnings();
  }

  private void beginTraceQuery(String cql, QueryState state, QueryOptions options) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("query", cql);
    if (options.getPageSize() > 0) {
      builder.put("page_size", Integer.toString(options.getPageSize()));
    }
    if (options.getConsistency() != null) {
      builder.put("consistency_level", options.getConsistency().name());
    }
    if (options.getSerialConsistency() != null) {
      builder.put("serial_consistency_level", options.getSerialConsistency().name());
    }

    Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
  }

  private void beginTraceExecute(
      ParsedStatement.Prepared prepared,
      QueryState state,
      org.apache.cassandra.cql3.QueryOptions options,
      ProtocolVersion version) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    if (options.getPageSize() > 0) {
      builder.put("page_size", Integer.toString(options.getPageSize()));
    }
    if (options.getConsistency() != null) {
      builder.put("consistency_level", options.getConsistency().name());
    }
    if (options.getSerialConsistency() != null) {
      builder.put("serial_consistency_level", options.getSerialConsistency().name());
    }
    builder.put("query", prepared.rawCQLStatement);

    for (int i = 0; i < prepared.boundNames.size(); i++) {
      ColumnSpecification cs = prepared.boundNames.get(i);
      String boundName = cs.name.toString();
      List<ByteBuffer> values = options.getValues();
      String boundValue = cs.type.asCQL3Type().toCQLLiteral(values.get(i), version);
      if (boundValue.length() > 1000) {
        boundValue = boundValue.substring(0, 1000) + "...'";
      }

      // Here we prefix boundName with the index to avoid possible collission in builder keys due to
      // having multiple boundValues for the same variable
      builder.put("bound_var_" + Integer.toString(i) + "_" + boundName, boundValue);
    }

    Tracing.instance.begin(
        "Execute CQL3 prepared query", state.getClientAddress(), builder.build());
  }

  private void beginTracePrepare(String cql, QueryState state) {
    Tracing.instance.begin(
        "Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", cql));
  }

  private void beginTraceBatch(QueryState state, QueryOptions options) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    if (options.getConsistency() != null) {
      builder.put("consistency_level", options.getConsistency().name());
    }
    if (options.getSerialConsistency() != null) {
      builder.put("serial_consistency_level", options.getSerialConsistency().name());
    }

    // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add
    // support.
    Tracing.instance.begin(
        "Execute batch of CQL3 queries", state.getClientAddress(), builder.build());
  }

  /**
   * When "cassandra.join_ring" is "false" {@link StorageService#initServer()} will not wait for
   * schema to propagate to the coordinator only node. This method fixes that limitation by waiting
   * for at least one backend ring member to become available and for their schemas to agree before
   * allowing initialization to continue.
   */
  private void waitForSchema(int delayMillis) {
    boolean isConnectedAndInAgreement = false;
    for (int i = 0; i < delayMillis; i += 1000) {
      if (Gossiper.instance.getLiveTokenOwners().size() > 0 && isInSchemaAgreement()) {
        logger.debug(
            "current schema version: {}", org.apache.cassandra.config.Schema.instance.getVersion());
        isConnectedAndInAgreement = true;
        break;
      }

      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    if (!isConnectedAndInAgreement) {
      logger.warn(
          "Unable to connect to live token owner and/or reach schema agreement after {} milliseconds",
          delayMillis);
    }
  }
}
