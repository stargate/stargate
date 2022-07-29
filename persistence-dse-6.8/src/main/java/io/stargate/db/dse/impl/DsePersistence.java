package io.stargate.db.dse.impl;

import static io.stargate.db.dse.impl.Conversion.toPreparedMetadata;
import static io.stargate.db.dse.impl.Conversion.toResultMetadata;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.db.nodes.Nodes;
import com.datastax.bdp.db.util.ProductType;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.stargate.auth.AuthorizationService;
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
import io.stargate.db.datastore.common.AbstractCassandraPersistence;
import io.stargate.db.dse.impl.idempotency.IdempotencyAnalyzer;
import io.stargate.db.dse.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;
import io.stargate.db.schema.TableName;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.IAuthContext;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
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
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.transport.Message.Request;
import org.apache.cassandra.transport.ServerConnection;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.flow.RxThreads;
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

  public static final Boolean USE_PROXY_PROTOCOL =
      Boolean.parseBoolean(System.getProperty("stargate.use_proxy_protocol", "false"));
  private static final boolean USE_TRANSITIONAL_AUTH =
      Boolean.getBoolean("stargate.cql_use_transitional_auth");
  private static final String EXTERNAL_USER_ROLE_NAME =
      System.getProperty(
          "stargate.auth.proxy.external.users.as", CassandraRoleManager.DEFAULT_SUPERUSER_NAME);
  private static final AuthenticatedUser EXTERNAL_AUTH_USER =
      new ExternalAuthenticatedUser(EXTERNAL_USER_ROLE_NAME);

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
  private AtomicReference<AuthorizationService> authorizationService;

  public DsePersistence() {
    super("DataStax Enterprise");
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
    // DSE picks this property during the static loading of the ClientState class. So we set it
    // early, to make sure that class is not loaded before we've set it.
    System.setProperty(
        "cassandra.custom_query_handler_class", StargateQueryHandler.class.getName());

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

    String dseConfigPath = System.getProperty("stargate.unsafe.dse_config_path", "");
    if (!dseConfigPath.isEmpty()) {
      System.setProperty("dse.config", new File(dseConfigPath).toURI().toString());
      DseConfig.init();
    }

    String hostId = System.getProperty("stargate.host_id");
    if (hostId != null && !hostId.isEmpty()) {
      try {
        Nodes.local().update(l -> l.setHostId(UUID.fromString(hostId)), true);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid host ID '%s': not a valid UUID", hostId), e);
      }
    }

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

    interceptor = new DefaultQueryInterceptor();
    if (USE_PROXY_PROTOCOL) interceptor = new ProxyProtocolQueryInterceptor(interceptor);

    interceptor.initialize();
    stargateHandler().register(interceptor);
    stargateHandler().setAuthorizationService(this.authorizationService);

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
    SchemaManager.instance.registerListener(new EventListenerWrapper(listener));
    interceptor.register(listener);
  }

  @Override
  public Authenticator getAuthenticator() {
    return authenticator;
  }

  @Override
  public void setRpcReady(boolean status) {
    StorageService.instance.setNativeTransportReady(status);
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
    // errors
    // with INCOMPATIBLE_SCHEMA.

    // Collect schema IDs from all relevant nodes and check that we have at most 1 distinct ID.
    return Gossiper.instance.getLiveMembers().stream()
            .filter(DsePersistence::shouldCheckSchema)
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
            .filter(DsePersistence::shouldCheckSchema)
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
            .filter(DsePersistence::shouldCheckSchema)
            .filter(DsePersistence::isStorageNode)
            .map(Gossiper.instance::getSchemaVersion)
            .distinct()
            .count()
        <= 1;
  }

  @Override
  public boolean isSchemaAgreementAchievable() {
    try {
      if (isInSchemaAgreement()) {
        return true;
      }

      if (!isStorageInSchemaAgreement()) {
        // Assume storage nodes are still disseminating schema data and will agree eventually.
        // Note: isSchemaAgreementAchievable() is used to determine if the Stargate node should
        // be restarted. In case storage nodes do not agree among themselves, restarting Stargate
        // will not help, so there's no point returning false in this situation even if the
        // schema disagreement at storage level is perpetual.
        logger.debug("Storage nodes are not in schema agreement");
        return true;
      }

      // At this point storage nodes agree among themselves,
      // but Stargate has a different schema version.
      Set<?> outstandingRequests = PullRequestGetter.nonCompletedPullRequests();
      if (!outstandingRequests.isEmpty()) {
        // Some schema pull tasks are still in progress or scheduled for execution.
        // Assume schema may be synchronized later.
        return true;
      }

      // No outstanding pull tasks (including retries).
      // Note: in practice schema pulls can be retried for a rather long period of time.
      logger.error("Schema agreement is not achievable");
      return false;

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean supportsSAI() {
    return true;
  }

  @Override
  public Map<String, List<String>> cqlSupportedOptions() {
    List<String> pageUnits =
        Arrays.stream(PageSize.PageUnit.values()).map(Enum::toString).collect(Collectors.toList());
    return ImmutableMap.<String, List<String>>builder()
        .put(StartupMessage.PAGE_UNIT, pageUnits)
        .put(
            StartupMessage.SERVER_VERSION,
            Collections.singletonList(ProductVersion.getDSEVersionString()))
        .put(StartupMessage.PRODUCT_TYPE, Collections.singletonList(ProductType.product.name()))
        .put(
            StartupMessage.EMULATE_DBAAS_DEFAULTS,
            Collections.singletonList(String.valueOf(DatabaseDescriptor.isEmulateDbaasDefaults())))
        .put(StartupMessage.CQL_VERSION, ImmutableList.of(QueryProcessor.CQL_VERSION.toString()))
        .build();
  }

  @Override
  public void executeAuthResponse(Runnable handler) {
    RxThreads.subscribeOnIo(Completable.fromRunnable(handler), TPCTaskType.AUTHENTICATION)
        .subscribe();
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

  public void setAuthorizationService(AtomicReference<AuthorizationService> authorizationService) {
    this.authorizationService = authorizationService;
  }

  private class DseConnection extends AbstractConnection {

    private final StargateClientState clientState;
    private final ServerConnection fakeServerConnection;

    private DseConnection(@Nonnull ClientInfo clientInfo) {
      this(clientInfo, StargateClientState.forExternalCalls(clientInfo));
    }

    private DseConnection() {
      this(null, StargateClientState.forInternalCalls());
    }

    @SuppressWarnings("RxReturnValueIgnored")
    private DseConnection(@Nullable ClientInfo clientInfo, StargateClientState clientState) {
      super(clientInfo);
      this.clientState = clientState;
      this.fakeServerConnection =
          new FakeConnection(
              clientInfo == null ? null : clientInfo.remoteAddress(),
              org.apache.cassandra.transport.ProtocolVersion.CURRENT);

      if (!authenticator.requireAuthentication()) {
        this.clientState.login(AuthenticatedUser.ANONYMOUS_USER);
      }
    }

    @Override
    public Persistence persistence() {
      return DsePersistence.this;
    }

    @Override
    @SuppressWarnings("RxReturnValueIgnored")
    protected void loginInternally(io.stargate.db.AuthenticatedUser user) {
      try {
        Single<ClientState> loginSingle;
        if (user.isFromExternalAuth() && USE_TRANSITIONAL_AUTH) {
          loginSingle = this.clientState.login(EXTERNAL_AUTH_USER);
          clientState.setExternalAuth(true);
        } else {
          loginSingle = this.clientState.login(new AuthenticatedUser(user.name()));
        }

        // For now, we're blocking as the login() API is synchronous. If this is a problem, we may
        // have to make said API asynchronous, but it makes things a tad more complex.
        @SuppressWarnings("unused")
        ClientState unused = loginSingle.blockingGet();

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
        // This is here to maintain consistent behavior with C* 3.11 and C* 4.0 since DSE requires
        // at least some user to be present but you cannot login with the "system" user.
        return Single.just(
            new QueryState(
                StargateClientState.forExternalCalls(AuthenticatedUser.ANONYMOUS_USER),
                UserRolesAndPermissions.SYSTEM));
      } else if (clientState.isExternalAuth()) {
        return Single.just(
            new QueryState(clientState, ExternallyAuthenticatedUserRole.INSTANCE.get()));
      } else if (clientState.getUser().isAnonymous()) {
        return Single.just(new QueryState(clientState, UserRolesAndPermissions.SYSTEM));
      } else {
        return DatabaseDescriptor.getAuthManager()
            .getUserRolesAndPermissions(
                clientState.getUser().getName(), clientState.getUser().getName(), IAuthContext.ANY)
            .map(u -> new QueryState(clientState, u));
      }
    }

    private <T extends Result> CompletableFuture<T> executeRequest(
        Parameters parameters, long queryStartNanoTime, Supplier<Request> requestSupplier) {

      try {
        // When running inside DSE, query tasks clear ExecutorLocals before
        // running, which is handled by its Message.channelRead0. In Stargate
        // requests may come from Epoll threads of the CQL module, from HTTP
        // threads or any other client. So here we ensure that DSE code starts
        // processing the request with a clean thread local state.
        ExecutorLocals.set(null);

        if (parameters.protocolVersion().isGreaterOrEqualTo(ProtocolVersion.V4)) {
          ClientWarn.instance.captureWarnings();
        }

        Single<QueryState> queryState = newQueryState();

        Request request = requestSupplier.get();
        if (parameters.tracingRequested()) {
          request.setTracingRequested();
        }
        request.setCustomPayload(parameters.customPayload().orElse(null));
        request.attach(fakeServerConnection);

        CompletableFuture<T> future = new CompletableFuture<>();
        @SuppressWarnings("unused")
        Disposable unused =
            request
                .execute(queryState, queryStartNanoTime)
                .map(
                    response -> {
                      // There is only 2 types of response that can come out: either a
                      // ResultMessage (which itself can of different kind), or an ErrorMessage.
                      if (response instanceof ErrorMessage) {
                        throw convertExceptionWithWarnings(
                            (Throwable) ((ErrorMessage) response).error);
                      }

                      @SuppressWarnings("unchecked")
                      T result =
                          (T)
                              Conversion.toResult(
                                  (ResultMessage) response,
                                  Conversion.toInternal(parameters.protocolVersion()),
                                  ClientWarn.instance.getAndClearWarnings());
                      return result;
                    })
                // doFinally runs after onComplete or onError
                // thus after the lambdas in subscribe below
                .doFinally(
                    () -> {
                      // this is to clean the thread local in TPC thread
                      ClientWarn.instance.resetWarnings();
                    })
                .subscribe(
                    future::complete,
                    ex -> {
                      if (!(ex instanceof PersistenceException)) {
                        ex = convertExceptionWithWarnings(ex);
                      }
                      future.completeExceptionally(ex);
                    });
        return future;
      } catch (Exception e) {
        CompletableFuture<T> exceptionalFuture = new CompletableFuture<>();
        exceptionalFuture.completeExceptionally(convertExceptionWithWarnings(e));
        return exceptionalFuture;
      } finally {
        // this is to clean the thread local in non-TPC thread.
        // note that: even though request execution happens asynchronously in TPC thread, but the
        // thread local
        // values should been already copied by TPC threads when enqueueing TPC tasks via {@link
        // ExecutorLocals#create()).
        ClientWarn.instance.resetWarnings();
      }
    }

    private PersistenceException convertExceptionWithWarnings(Throwable t) {
      PersistenceException pe = Conversion.convertInternalException(t);
      pe.setWarnings(ClientWarn.instance.getAndClearWarnings());
      return pe;
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
    public Result.Prepared getPrepared(String query, Parameters parameters) {
      QueryHandler handler = ClientState.getCQLQueryHandler();
      String keyspace = parameters.defaultKeyspace().orElse(clientState.getRawKeyspace());
      String toHash = keyspace == null ? query : keyspace + query;
      MD5Digest statementId = MD5Digest.compute(toHash);
      QueryHandler.Prepared existing = handler.getPrepared(statementId);

      if (existing != null) {
        CQLStatement statement = existing.statement;
        boolean idempotent = IdempotencyAnalyzer.isIdempotent(statement);
        boolean useKeyspace = statement instanceof UseStatement;
        ResultSet.PreparedMetadata metadata = ResultSet.PreparedMetadata.fromStatement(statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromStatement(statement);
        return new Result.Prepared(
            Conversion.toExternal(statementId),
            Conversion.toExternal(existing.resultMetadataId),
            toResultMetadata(resultMetadata, null),
            toPreparedMetadata(metadata.names, statement.getPartitionKeyBindVariableIndexes()),
            idempotent,
            useKeyspace);
      }

      return null;
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

    @Override
    public ByteBuffer makePagingState(PagingPosition pos, Parameters parameters) {
      TableMetadata table =
          SchemaManager.instance.validateTable(pos.tableName().keyspace(), pos.tableName().name());
      return Conversion.toPagingState(table, pos, parameters);
    }

    @Override
    public RowDecorator makeRowDecorator(TableName tableName) {
      TableMetadata table =
          SchemaManager.instance.validateTable(tableName.keyspace(), tableName.name());
      return new RowDecoratorImpl(tableName, table);
    }

    private Object queryOrId(Statement statement) {
      if (statement instanceof SimpleStatement) {
        return ((SimpleStatement) statement).queryString();
      } else {
        return Conversion.toInternal(((BoundStatement) statement).preparedId());
      }
    }
  }

  private static class PullRequestGetter {
    private static final Method nonCompletedPullRequestsMethod;
    private static final Object scheduler; // PullRequestScheduler

    static {
      try {
        Field schedulerField = MigrationManager.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        scheduler = schedulerField.get(MigrationManager.instance);
        nonCompletedPullRequestsMethod =
            scheduler.getClass().getDeclaredMethod("nonCompletedPullRequest");
        nonCompletedPullRequestsMethod.setAccessible(true);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    private static Set<?> nonCompletedPullRequests() {
      try {
        return (Set<?>) nonCompletedPullRequestsMethod.invoke(scheduler);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static class ExternalAuthenticatedUser extends AuthenticatedUser {

    public ExternalAuthenticatedUser(String roleName) {
      // Note: this user is both system and anonymous and is expected to have a superuser role name.
      //
      // This is needed for properly forwarding DDL to storage nodes, which is controlled by
      // SchemaLeaderManager.SCHEMA_CHANGE_FORWARDING_ENABLED.
      //
      // Access control for external users (including DDL and DML statements) is performed before
      // the superuser role of this user object comes into play (cf. StargateQueryHandler).
      super(RoleResource.role(roleName), true, true, null);
    }
  }
}
