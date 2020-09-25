package io.stargate.db.dse.impl;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Single;
import io.stargate.db.Authenticator;
import io.stargate.db.Batch;
import io.stargate.db.BoundStatement;
import io.stargate.db.ClientInfo;
import io.stargate.db.EventListener;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.datastore.common.AbstractCassandraPersistence;
import io.stargate.db.dse.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewTableMetadata;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DsePersistence
    extends AbstractCassandraPersistence<
        Config,
        KeyspaceMetadata,
        TableMetadata,
        ColumnMetadata,
        UserType,
        IndexMetadata,
        ViewTableMetadata> {
  private static final Logger logger = LoggerFactory.getLogger(DsePersistence.class);

  static {
    System.setProperty(
        "cassandra.custom_query_handler_class", StargateQueryHandler.class.getName());
  }

  public static final Boolean USE_PROXY_PROTOCOL =
      Boolean.parseBoolean(System.getProperty("stargate.use_proxy_protocol", "false"));

  /*
   * Initial schema migration can take greater than 2 * MigrationManager.MIGRATION_DELAY_IN_MS if a
   * live token owner doesn't become live within MigrationManager.MIGRATION_DELAY_IN_MS.
   */
  private static final int STARTUP_DELAY_MS =
      Integer.getInteger("stargate.startup_delay_ms", 3 * MigrationManager.MIGRATION_DELAY_IN_MS);

  private CassandraDaemon cassandraDaemon;
  private Authenticator authenticator;
  private QueryInterceptor interceptor;

  // C* listener that ensures that our Stargate schema remains up-to-date with the internal C* one.
  private SchemaChangeListener schemaChangeListener;

  public DsePersistence() {
    super("DataStax Enterprise");
  }

  private StargateQueryHandler stargateHandler() {
    return (StargateQueryHandler) ClientState.getCQLQueryHandler();
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

    schemaChangeListener =
        new SimpleCallbackSchemaChangeListener() {
          @Override
          void onSchemaChange() {
            runOnSchemaChange.run();
          }
        };
    org.apache.cassandra.schema.SchemaManager.instance.registerListener(schemaChangeListener);
  }

  @Override
  protected void unregisterInternalSchemaListener() {
    if (schemaChangeListener != null) {
      org.apache.cassandra.schema.SchemaManager.instance.unregisterListener(schemaChangeListener);
    }
  }

  @Override
  protected void initializePersistence(Config config) {
    // Need to set this to true otherwise org.apache.cassandra.service.CassandraDaemon#activate0
    // will close both System.out and System.err.
    System.setProperty("cassandra-foreground", "true");
    System.setProperty("cassandra.consistent.rangemovement", "false");

    if (Boolean.parseBoolean(System.getProperty("stargate.bind_to_listen_address"))) {
      // Bind JMX server to listen address
      System.setProperty(
          "com.sun.management.jmxremote.host", System.getProperty("stargate.listen_address"));
    }

    DatabaseDescriptor.daemonInitialization(true, config);
    cassandraDaemon = new CassandraDaemon(true);

    // CassandraDaemon.activate() creates a thread that swallows exceptions that occur during
    // startup. Use
    // an UnauthorizedExceptionHandler to check for failure.
    AtomicReference<Throwable> throwableFromMainThread = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, t) -> {
          if (thread.getName().equals("DSE main thread")) {
            throwableFromMainThread.set(t);
          }
        });

    cassandraDaemon.activate(false);
    Throwable t = throwableFromMainThread.get();
    if (t != null) {
      // Stop initialization if DSE is not started
      throw new RuntimeException("Unable to start DSE persistence layer", t);
    }

    // Use special gossip state "X10" to differentiate stargate nodes
    Gossiper.instance.addLocalApplicationState(
        ApplicationState.X10, StorageService.instance.valueFactory.dsefsState("stargate"));

    waitForSchema(STARTUP_DELAY_MS);

    if (USE_PROXY_PROTOCOL) interceptor = new ProxyProtocolQueryInterceptor();
    else interceptor = new DefaultQueryInterceptor();

    interceptor.initialize();
    stargateHandler().register(interceptor);

    authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
  }

  @Override
  protected void destroyPersistence() {
    if (cassandraDaemon != null) {
      cassandraDaemon.deactivate();
      cassandraDaemon = null;
    }
  }

  @Override
  public void registerEventListener(EventListener listener) {
    EventListenerWrapper wrapper = new EventListenerWrapper(listener);
    SchemaManager.instance.registerListener(wrapper);
    interceptor.register(wrapper);
  }

  @Override
  public boolean isRpcReady(InetAddressAndPort endpoint) {
    return StorageService.instance.isRpcReady(endpoint.address);
  }

  @Override
  public InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint) {
    try {
      return InetAddressAndPort.getByName(
          StorageService.instance.getNativeTransportAddress(endpoint.address));
    } catch (UnknownHostException e) {
      // That should not happen, so log an error, but return the
      // endpoint address since there's a good change this is right
      logger.error("Problem retrieving RPC address for {}", endpoint, e);
      return InetAddressAndPort.getByAddressOverrideDefaults(
          endpoint.address, DatabaseDescriptor.getNativeTransportPort());
    }
  }

  @Override
  public Authenticator getAuthenticator() {
    return authenticator;
  }

  @Override
  public Connection newConnection(ClientInfo clientInfo) {
    return new DseConnection(clientInfo);
  }

  @Override
  public Connection newConnection() {
    return new DseConnection();
  }

  @Override
  public ByteBuffer unsetValue() {
    return ByteBufferUtil.UNSET_BYTE_BUFFER;
  }

  @Override
  public boolean isInSchemaAgreement() {
    // We only include live nodes because this method is mainly used to wait for schema
    // agreement, and waiting for failed nodes is not a great idea.
    // Also note that in theory getSchemaVersion can return null for some nodes, and if it does
    // the code below will likely return false (the null will be an element on its own), but that's
    // probably the right answer in that case. In practice, this shouldn't be a problem though.

    // Important: This must include all nodes including fat clients, otherwise we'll get write
    // errors
    // with INCOMPATIBLE_SCHEMA.
    return Gossiper.instance.getLiveMembers().stream()
            .filter(
                ep -> {
                  EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
                  return epState != null && !Gossiper.instance.isDeadState(epState);
                })
            .map(Gossiper.instance::getSchemaVersion)
            .collect(Collectors.toSet())
            .size()
        <= 1;
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
        logger.debug("current schema version: {}", SchemaManager.instance.getVersion());
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

  private static ClientState clientStateForExternalCalls(@Nonnull ClientInfo clientInfo) {
    if (clientInfo.publicAddress().isPresent()) {
      new ClientStateWithPublicAddress(
          clientInfo.remoteAddress(), clientInfo.publicAddress().get());
    }
    return ClientState.forExternalCalls(clientInfo.remoteAddress(), null);
  }

  private class DseConnection extends AbstractConnection {
    private final ClientState clientState;

    private DseConnection(@Nonnull ClientInfo clientInfo) {
      this(clientInfo, clientStateForExternalCalls(clientInfo));
    }

    private DseConnection() {
      this(null, ClientState.forInternalCalls());
    }

    private DseConnection(@Nullable ClientInfo clientInfo, ClientState clientState) {
      super(clientInfo);
      this.clientState = clientState;

      if (!authenticator.requireAuthentication()) {
        clientState.login(AuthenticatedUser.ANONYMOUS_USER);
      }
    }

    @Override
    public Persistence persistence() {
      return DsePersistence.this;
    }

    @Override
    protected void loginInternally(io.stargate.db.AuthenticatedUser user) {
      try {
        // For now, we're blocking as the login() API is synchronous. If this is a problem, we may
        // have to make said API asynchronous, but it makes things a tad more complex.
        clientState.login(new AuthenticatedUser(user.name())).blockingGet();
      } catch (AuthenticationException e) {
        throw new org.apache.cassandra.stargate.exceptions.AuthenticationException(e);
      }
    }

    @Override
    public Optional<String> usedKeyspace() {
      return Optional.ofNullable(clientState.getRawKeyspace());
    }

    private Single<QueryState> newQueryState() {
      if (clientState.getUser() == null) {
        return Single.just(new QueryState(clientState, UserRolesAndPermissions.ANONYMOUS));
      } else {
        return DatabaseDescriptor.getAuthManager()
            .getUserRolesAndPermissions(
                clientState.getUser().getName(), clientState.getUser().getName())
            .map(u -> new QueryState(clientState, u));
      }
    }

    private <T extends Result> CompletableFuture<T> executeRequest(
        Parameters parameters, long queryStartNanoTime, Supplier<Request> requestSupplier) {

      try {
        Single<QueryState> queryState = newQueryState();
        Request request = requestSupplier.get();
        if (parameters.tracingRequested()) {
          request.setTracingRequested();
        }
        request.setCustomPayload(parameters.customPayload().orElse(null));

        return Conversion.toFuture(
            request
                .execute(queryState, queryStartNanoTime)
                .map(
                    response -> {
                      // There is only 2 types of response that can come out: either a ResutMessage
                      // (which itself can of different kind), or an ErrorMessage.
                      if (response instanceof ErrorMessage) {
                        throw new UncheckedExecutionException(
                            Conversion.convertInternalException(
                                (Throwable) ((ErrorMessage) response).error));
                      }

                      return (T)
                          Conversion.toResult(
                              (ResultMessage) response,
                              Conversion.toInternal(parameters.protocolVersion()));
                    }));
      } catch (Exception e) {
        CompletableFuture<T> exceptionalFuture = new CompletableFuture<>();
        exceptionalFuture.completeExceptionally(Conversion.convertInternalException(e));
        return exceptionalFuture;
      }
    }

    @Override
    public CompletableFuture<Result> execute(
        Statement statement, Parameters parameters, long queryStartNanoTime) {
      return executeRequest(
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
              org.apache.cassandra.utils.MD5Digest id =
                  Conversion.toInternal(((BoundStatement) statement).preparedId());
              // The 'resultMetadataId' is a protocol v5 feature we don't yet support
              return new ExecuteMessage(id, null, options);
            }
          });
    }

    @Override
    public CompletableFuture<Result.Prepared> prepare(String query, Parameters parameters) {
      return executeRequest(
          parameters,
          // The queryStartNanoTime is not used by prepared message, so it doesn't really matter
          // that it's only computed now.
          System.nanoTime(),
          () -> new PrepareMessage(query, parameters.defaultKeyspace().orElse(null)));
    }

    @Override
    public CompletableFuture<Result> batch(
        Batch batch, Parameters parameters, long queryStartNanoTime) {

      return executeRequest(
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

    private Object queryOrId(Statement statement) {
      if (statement instanceof SimpleStatement) {
        return ((SimpleStatement) statement).queryString();
      } else {
        return Conversion.toInternal(((BoundStatement) statement).preparedId());
      }
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
  }
}
