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

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.util.TimeSource;
import io.stargate.db.Authenticator;
import io.stargate.db.Batch;
import io.stargate.db.BoundStatement;
import io.stargate.db.ClientInfo;
import io.stargate.db.EventListener;
import io.stargate.db.PagingPosition;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.RowDecorator;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.cassandra.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
import io.stargate.db.datastore.common.AbstractCassandraPersistence;
import io.stargate.db.datastore.common.util.SchemaAgreementAchievableCheck;
import io.stargate.db.schema.TableName;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra311Persistence
    extends AbstractCassandraPersistence<
        Config,
        KeyspaceMetadata,
        CFMetaData,
        ColumnDefinition,
        UserType,
        IndexMetadata,
        ViewDefinition> {

  private static final Logger logger = LoggerFactory.getLogger(Cassandra311Persistence.class);

  // copied from org.apache.cassandra.service.MigrationCoordinator.MIGRATION_DELAY_IN_MS
  // please keep in sync
  private static final int MIGRATION_DELAY_IN_MS = 60000;

  private static final boolean USE_TRANSITIONAL_AUTH =
      Boolean.getBoolean("stargate.cql_use_transitional_auth");

  /*
   * Initial schema migration can take greater than 2 * MigrationManager.MIGRATION_DELAY_IN_MS if a
   * live token owner doesn't become live within MigrationManager.MIGRATION_DELAY_IN_MS. Because it's
   * unknown how long a schema migration takes this waits for an extra MIGRATION_DELAY_IN_MS.
   */
  private static final int STARTUP_DELAY_MS =
      Integer.getInteger("stargate.startup_delay_ms", 3 * MIGRATION_DELAY_IN_MS);

  // SCHEMA_SYNC_GRACE_PERIOD should be longer than MigrationManager.MIGRATION_DELAY_IN_MS to allow
  // the schema pull tasks to be initiated, plus some time for executing the pull requests plus
  // some time to merge the responses. By default the pull task timeout is equal to
  // DatabaseDescriptor.getRpcTimeout() (10 sec) and there are no retries. We assume that the merge
  // operation should complete within the default MIGRATION_DELAY_IN_MS.
  private static final Duration SCHEMA_SYNC_GRACE_PERIOD =
      Duration.ofMillis(
          Long.getLong("stargate.schema_sync_grace_period_ms", 2 * MIGRATION_DELAY_IN_MS + 10_000));

  private final SchemaCheck schemaCheck = new SchemaCheck();

  private LocalAwareExecutorService executor;

  private CassandraDaemon daemon;
  private Authenticator authenticator;
  private QueryInterceptor interceptor;

  // C* listener that ensures that our Stargate schema remains up-to-date with the internal C* one.
  private MigrationListener migrationListener;
  private AtomicReference<AuthorizationService> authorizationService;

  public Cassandra311Persistence() {
    super("Apache Cassandra");
  }

  private StargateQueryHandler stargateHandler() {
    return (StargateQueryHandler) ClientState.getCQLQueryHandler();
  }

  @Override
  protected SchemaConverter newSchemaConverter() {
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
    // C* picks this property during the static loading of the ClientState class. So we set it
    // early, to make sure that class is not loaded before we've set it.
    System.setProperty(
        "cassandra.custom_query_handler_class", StargateQueryHandler.class.getName());

    daemon = new CassandraDaemon(true);

    DatabaseDescriptor.daemonInitialization(() -> config);
    try {
      daemon.init(null);
    } catch (IOException e) {
      throw new RuntimeException("Unable to start Cassandra persistence layer", e);
    }

    String hostId = System.getProperty("stargate.host_id");
    if (hostId != null && !hostId.isEmpty()) {
      try {
        SystemKeyspace.setLocalHostId(UUID.fromString(hostId));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid host ID '%s': not a valid UUID", hostId), e);
      }
    }

    executor =
        SHARED.newExecutor(
            DatabaseDescriptor.getNativeTransportMaxThreads(),
            "transport",
            "Native-Transport-Requests");

    // Use special gossip state "X10" to differentiate stargate nodes
    Gossiper.instance.addLocalApplicationState(
        ApplicationState.X10, StorageService.instance.valueFactory.releaseVersion("stargate"));

    Gossiper.instance.register(schemaCheck);

    daemon.start();

    waitForSchema(STARTUP_DELAY_MS);

    authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
    interceptor = new DefaultQueryInterceptor();
    interceptor.initialize();
    stargateHandler().register(interceptor);
    stargateHandler().setAuthorizationService(this.authorizationService);
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
    MigrationManager.instance.register(new EventListenerWrapper(listener));
    interceptor.register(listener);
  }

  @Override
  public ByteBuffer unsetValue() {
    return ByteBufferUtil.UNSET_BYTE_BUFFER;
  }

  @Override
  public Authenticator getAuthenticator() {
    return authenticator;
  }

  @Override
  public void setRpcReady(boolean status) {
    StorageService.instance.setRpcReady(status);
  }

  @Override
  public Connection newConnection(ClientInfo clientInfo) {
    return new Cassandra311Connection(clientInfo);
  }

  @Override
  public Connection newConnection() {
    return new Cassandra311Connection();
  }

  private <T extends Result> CompletableFuture<T> runOnExecutor(
      Supplier<T> supplier, boolean captureWarnings) {
    assert executor != null : "This persistence has not been initialized";
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.submit(
        () -> {
          if (captureWarnings) {
            ClientWarn.instance.captureWarnings();
          }
          try {
            @SuppressWarnings("unchecked")
            T resultWithWarnings =
                (T) supplier.get().setWarnings(ClientWarn.instance.getWarnings());
            future.complete(resultWithWarnings);
          } catch (Throwable t) {
            JVMStabilityInspector.inspectThrowable(t);
            PersistenceException pe =
                (t instanceof PersistenceException)
                    ? (PersistenceException) t
                    : Conversion.convertInternalException(t);
            pe.setWarnings(ClientWarn.instance.getWarnings());
            future.completeExceptionally(pe);
          } finally {
            // Note that it's a no-op if we haven't called captureWarnings
            ClientWarn.instance.resetWarnings();
          }
        });

    return future;
  }

  private static boolean shouldCheckSchema(InetAddress ep) {
    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
    return epState != null && !Gossiper.instance.isDeadState(epState);
  }

  private static boolean isStorageNode(InetAddress ep) {
    return !Gossiper.instance.isGossipOnlyMember(ep);
  }

  @Override
  public boolean isInSchemaAgreement() {
    // We only include live nodes because this method is mainly used to wait for schema
    // agreement, and waiting for failed nodes is not a great idea.
    // Also note that in theory getSchemaVersion can return null for some nodes, and if it does
    // the code below will likely return false (the null will be an element on its own), but that's
    // probably the right answer in that case. In practice, this shouldn't be a problem though.

    // Important: This must include all nodes including fat clients, otherwise we'll get write
    // errors with INCOMPATIBLE_SCHEMA.

    // Collect schema IDs from all relevant nodes and check that we have at most 1 distinct ID.
    return Gossiper.instance.getLiveMembers().stream()
            .filter(Cassandra311Persistence::shouldCheckSchema)
            .map(Gossiper.instance::getSchemaVersion)
            .distinct()
            .count()
        <= 1;
  }

  @Override
  public boolean isInSchemaAgreementWithStorage() {
    // Collect schema IDs from storage and local node and check that we have at most 1 distinct ID
    InetAddress localAddress = FBUtilities.getBroadcastAddress();
    return Gossiper.instance.getLiveMembers().stream()
            .filter(Cassandra311Persistence::shouldCheckSchema)
            .filter(ep -> isStorageNode(ep) || localAddress.equals(ep))
            .map(Gossiper.instance::getSchemaVersion)
            .distinct()
            .count()
        <= 1;
  }

  /**
   * This method indicates whether storage nodes (i.e. excluding Stargate) agree on the schema
   * version among themselves.
   */
  @VisibleForTesting
  boolean isStorageInSchemaAgreement() {
    // Collect schema IDs from storage nodes and check that we have at most 1 distinct ID.
    return Gossiper.instance.getLiveMembers().stream()
            .filter(Cassandra311Persistence::shouldCheckSchema)
            .filter(Cassandra311Persistence::isStorageNode)
            .map(Gossiper.instance::getSchemaVersion)
            .distinct()
            .count()
        <= 1;
  }

  @Override
  public boolean isSchemaAgreementAchievable() {
    return schemaCheck.check();
  }

  @Override
  public boolean supportsSAI() {
    return false;
  }

  @Override
  public Map<String, List<String>> cqlSupportedOptions() {
    return ImmutableMap.<String, List<String>>builder()
        .put(StartupMessage.CQL_VERSION, ImmutableList.of(QueryProcessor.CQL_VERSION.toString()))
        .build();
  }

  @Override
  public void executeAuthResponse(Runnable handler) {
    executor.execute(handler);
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

  public void setAuthorizationService(AtomicReference<AuthorizationService> authorizationService) {
    this.authorizationService = authorizationService;
  }

  private class Cassandra311Connection extends AbstractConnection {

    private final ClientState clientState;

    private Cassandra311Connection(@Nonnull ClientInfo clientInfo) {
      this(clientInfo, ClientState.forExternalCalls(clientInfo.remoteAddress()));
    }

    private Cassandra311Connection() {
      this(null, ClientState.forInternalCalls());
    }

    private Cassandra311Connection(@Nullable ClientInfo clientInfo, ClientState clientState) {
      super(clientInfo);
      this.clientState = clientState;

      if (!authenticator.requireAuthentication()) {
        clientState.login(AuthenticatedUser.ANONYMOUS_USER);
      }
    }

    @Override
    public Persistence persistence() {
      return Cassandra311Persistence.this;
    }

    @Override
    protected void loginInternally(io.stargate.db.AuthenticatedUser user) {
      try {
        if (user.isFromExternalAuth() && USE_TRANSITIONAL_AUTH) {
          clientState.login(AuthenticatedUser.ANONYMOUS_USER);
        } else {
          clientState.login(new AuthenticatedUser(user.name()));
        }
      } catch (AuthenticationException e) {
        throw new org.apache.cassandra.stargate.exceptions.AuthenticationException(e);
      }
    }

    @Override
    public Optional<String> usedKeyspace() {
      return Optional.ofNullable(clientState.getRawKeyspace());
    }

    private <T extends Result> CompletableFuture<T> executeRequestOnExecutor(
        Parameters parameters, long queryStartNanoTime, Supplier<Request> requestSupplier) {
      return runOnExecutor(
          () -> {
            QueryState queryState =
                new QueryState(
                    parameters
                        .defaultKeyspace()
                        .map(k -> cloneWithKeyspace(clientState, k))
                        .orElse(clientState));
            Request request = requestSupplier.get();
            if (parameters.tracingRequested()) {
              request.setTracingRequested();
            }
            request.setCustomPayload(parameters.customPayload().orElse(null));
            Message.Response response = request.execute(queryState, queryStartNanoTime);
            // There is only 2 types of response that can come out: either a ResultMessage (which
            // itself can of different kind), or an ErrorMessage.
            if (response instanceof ErrorMessage) {
              // Note that we convert in runOnExecutor (to handle exceptions coming from other
              // parts of this method), but we need an unchecked exception here anyway, so
              // we convert, and runOnExecutor will detect it's already converted.
              throw Conversion.convertInternalException(
                  (Throwable) ((ErrorMessage) response).error);
            }
            @SuppressWarnings("unchecked")
            T result =
                (T)
                    Conversion.toResult(
                        (ResultMessage) response,
                        Conversion.toInternal(parameters.protocolVersion()));
            return result;
          },
          parameters.protocolVersion().isGreaterOrEqualTo(ProtocolVersion.V4));
    }

    private ClientState cloneWithKeyspace(ClientState original, String keyspace) {
      ClientState clone =
          original.isInternal
              ? ClientState.forInternalCalls()
              : ClientState.forExternalCalls(original.getRemoteAddress());
      clone.login(original.getUser());
      if (original.isNoCompactMode()) {
        clone.setNoCompactMode();
      }
      if (keyspace != null) {
        clone.setKeyspace(keyspace);
      }
      return clone;
    }

    @Override
    public CompletableFuture<Result> execute(
        Statement statement, Parameters parameters, long queryStartNanoTime) {
      return executeRequestOnExecutor(
          parameters,
          queryStartNanoTime,
          () -> {
            QueryOptions options =
                Conversion.toInternal(
                    statement.values(), statement.boundNames().orElse(null), parameters);

            if (statement instanceof SimpleStatement) {
              String queryString = ((SimpleStatement) statement).queryString();
              return new QueryMessage(queryString, options);
            } else {
              MD5Digest id = Conversion.toInternal(((BoundStatement) statement).preparedId());
              return new ExecuteMessage(id, options);
            }
          });
    }

    @Override
    public CompletableFuture<Result.Prepared> prepare(String query, Parameters parameters) {
      return executeRequestOnExecutor(
          parameters,
          // The queryStartNanoTime is not used by prepared message, so it doesn't really matter
          // that it's only computed now.
          System.nanoTime(),
          () -> new PrepareMessage(query));
    }

    @Override
    public CompletableFuture<Result> batch(
        Batch batch, Parameters parameters, long queryStartNanoTime) {
      return executeRequestOnExecutor(
          parameters,
          queryStartNanoTime,
          () -> {
            QueryOptions options = Conversion.toInternal(Collections.emptyList(), null, parameters);
            BatchStatement.Type internalBatchType = Conversion.toInternal(batch.type());
            List<Object> queryOrIdList = new ArrayList<>(batch.size());
            List<List<ByteBuffer>> allValues = new ArrayList<>(batch.size());

            for (Statement statement : batch.statements()) {
              queryOrIdList.add(queryOrId(statement));
              allValues.add(statement.values());
            }
            return new BatchMessage(internalBatchType, queryOrIdList, allValues, options);
          });
    }

    @Override
    public ByteBuffer makePagingState(PagingPosition position, Parameters parameters) {
      return Conversion.toPagingState(position, parameters);
    }

    @Override
    public RowDecorator makeRowDecorator(TableName tableName) {
      return new RowDecoratorImpl(tableName);
    }

    private Object queryOrId(Statement statement) {
      if (statement instanceof SimpleStatement) {
        return ((SimpleStatement) statement).queryString();
      } else {
        return Conversion.toInternal(((BoundStatement) statement).preparedId());
      }
    }
  }

  private class SchemaCheck extends SchemaAgreementAchievableCheck
      implements IEndpointStateChangeSubscriber {

    public SchemaCheck() {
      super(
          Cassandra311Persistence.this::isInSchemaAgreement,
          Cassandra311Persistence.this::isStorageInSchemaAgreement,
          SCHEMA_SYNC_GRACE_PERIOD,
          TimeSource.SYSTEM);
    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
      // Reset the schema sync grace period timeout on any schema change notifications
      // even if there are no actual changes.
      if (state == ApplicationState.SCHEMA) {
        reset();
      }
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState) {}

    @Override
    public void beforeChange(
        InetAddress endpoint,
        EndpointState currentState,
        ApplicationState newStateKey,
        VersionedValue newValue) {}

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state) {}

    @Override
    public void onDead(InetAddress endpoint, EndpointState state) {}

    @Override
    public void onRemove(InetAddress endpoint) {}

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state) {}
  }
}
