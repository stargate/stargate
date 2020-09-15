package io.stargate.db.dse.impl;

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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.util.SchemaTool;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Single;
import io.stargate.db.Authenticator;
import io.stargate.db.BatchType;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.dse.datastore.InternalDataStore;
import io.stargate.db.dse.impl.interceptors.DefaultQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.ProxyProtocolQueryInterceptor;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;

public class DsePersistence implements Persistence<Config, org.apache.cassandra.service.ClientState, org.apache.cassandra.service.QueryState>
{
    private static final Logger logger = LoggerFactory.getLogger(DsePersistence.class);

    public static final Boolean USE_PROXY_PROTOCOL = Boolean.parseBoolean(System.getProperty("stargate.use_proxy_protocol", "false"));

    private CassandraDaemon cassandraDaemon;
    private DataStore root;
    private Authenticator authenticator;
    private QueryHandler handler;
    private QueryInterceptor interceptor;

    @Override
    public String name()
    {
        return "DataStax Enterprise";
    }

    @Override
    public void initialize(Config config)
    {
        logger.info("Initializing DSE");
        System.setProperty("cassandra.join_ring", "false");
        // Need to set this to true otherwise org.apache.cassandra.service.CassandraDaemon#activate0 will close
        // both System.out and System.err.
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.consistent.rangemovement", "false");

        DatabaseDescriptor.daemonInitialization(true, config);
        cassandraDaemon = new CassandraDaemon(true);

        // CassandraDaemon.activate() creates a thread that swallows exceptions that occur during startup. Use
        // an UnauthorizedExceptionHandler to check for failure.
        AtomicReference<Throwable> throwableFromMainThread = new AtomicReference<>();
        Thread.setDefaultUncaughtExceptionHandler((thread, t) ->
        {
            if (thread.getName().equals("DSE main thread"))
            {
                throwableFromMainThread.set(t);
            }
        });

        cassandraDaemon.activate(false);
        Throwable t = throwableFromMainThread.get();
        if (t != null)
        {
            // Stop initialization if DSE is not started
            throw new RuntimeException("Unable to start DSE persistence layer", t);
        }

        // Use special gossip state "X10" to differentiate stargate nodes
        Gossiper.instance.addLocalApplicationState(ApplicationState.X10, StorageService.instance.valueFactory.dsefsState("stargate"));

        waitForSchema(StorageService.RING_DELAY);

        if (USE_PROXY_PROTOCOL)
            interceptor = new ProxyProtocolQueryInterceptor();
        else
            interceptor = new DefaultQueryInterceptor();

        interceptor.initialize();

        root = new InternalDataStore();
        authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
        handler = ClientState.getCQLQueryHandler();
    }

    @Override
    public void destroy()
    {
        if (cassandraDaemon != null)
        {
            root = null;
            cassandraDaemon.deactivate();
            cassandraDaemon = null;
        }
    }

    @Override
    public void registerEventListener(EventListener listener)
    {
        EventListenerWrapper wrapper = new EventListenerWrapper(listener);
        SchemaManager.instance.registerListener(wrapper);
        interceptor.register(wrapper);
    }

    @Override
    public boolean isRpcReady(InetAddressAndPort endpoint)
    {
        return StorageService.instance.isRpcReady(endpoint.address);
    }

    @Override
    public InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint)
    {
        try
        {
            return InetAddressAndPort.getByName(StorageService.instance.getNativeTransportAddress(endpoint.address));
        }
        catch (UnknownHostException e)
        {
            // That should not happen, so log an error, but return the
            // endpoint address since there's a good change this is right
            logger.error("Problem retrieving RPC address for {}", endpoint, e);
            return InetAddressAndPort.getByAddressOverrideDefaults(endpoint.address, DatabaseDescriptor.getNativeTransportPort());
        }
    }

    public DataStore newDataStore(QueryState state, QueryOptions queryOptions)
    {
        return new InternalDataStore(root, Conversion.toInternal(state), Conversion.toInternal(queryOptions));
    }

    @Override
    public QueryState newQueryState(io.stargate.db.ClientState clientState)
    {
        return new QueryStateWrapper(clientState);
    }

    @Override
    public io.stargate.db.ClientState<ClientState> newClientState(SocketAddress remoteAddress, InetSocketAddress publicAddress)
    {
        if (remoteAddress == null)
        {
            throw new IllegalArgumentException("No remote address provided");
        }

        if (authenticator.requireAuthentication())
        {
            return ClientStateWrapper.forExternalCalls(remoteAddress, publicAddress);
        }

        assert remoteAddress instanceof InetSocketAddress;
        return ClientStateWrapper.forExternalCalls(AuthenticatedUser.ANONYMOUS_USER, (InetSocketAddress) remoteAddress, publicAddress);
    }

    @Override
    public io.stargate.db.ClientState newClientState(String name)
    {
        if (Strings.isNullOrEmpty(name))
            return ClientStateWrapper.forInternalCalls();

        ClientStateWrapper state = ClientStateWrapper.forExternalCalls(null);
        state.login(new AuthenticatorWrapper.AuthenticatedUserWrapper(new AuthenticatedUser(name)));
        return state;
    }

    @Override
    public Authenticator getAuthenticator()
    {
        return authenticator;
    }

    @Override
    public CompletableFuture<? extends Result> query(String cql, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime)
    {
        try
        {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

            return Conversion.toFuture(Single.defer(() ->
            {
                try
                {
                    final UUID tracingId = beginTraceQuery(cql, internalState, internalOptions, customPayload, isTracingRequested);

                    checkIsLoggedIn(internalState);

                    CQLStatement statement = QueryProcessor.parseStatement(cql, internalState);

                    return processStatement(statement, state, options, customPayload, queryStartNanoTime, tracingId);
                }
                catch (Exception e)
                {
                    return Single.error(Conversion.handleException(e));
                }
            }));
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return stopTracingWithException(e);
        }
    }

    @Override
    public CompletableFuture<? extends Result> execute(MD5Digest id, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime)
    {
        try
        {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

            return Conversion.toFuture(Single.defer(() ->
            {
                try
                {
                    QueryHandler.Prepared prepared = handler.getPrepared(Conversion.toInternal(id));
                    if (prepared == null)
                    {
                        return Single.error(new PreparedQueryNotFoundException(id));
                    }

                    final CQLStatement statement = prepared.statement;
                    final UUID tracingId = beginTraceExecute(statement, internalState, internalOptions, customPayload, isTracingRequested);

                    checkIsLoggedIn(internalState);

                    return processStatement(statement, state, options, customPayload, queryStartNanoTime, tracingId);
                }
                catch (Exception e)
                {
                    return Single.error(Conversion.handleException(e));
                }
            }));
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return stopTracingWithException(e);
        }
    }

    @Override
    public CompletableFuture<? extends Result> prepare(String cql, QueryState state, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        try
        {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

            final UUID tracingId = beginTracePrepare(cql, internalState, customPayload, isTracingRequested);

            return Conversion.toFuture(Single.defer(() ->
            {
                checkIsLoggedIn(internalState);
                return handler.prepare(cql, Conversion.toInternal(state), customPayload);
            })
                    .map((result) -> Conversion.toPrepared(result).setTracingId(tracingId))
                    .flatMap(result -> Tracing.instance.stopSessionAsync().toSingleDefault(result))
                    .onErrorResumeNext((e) -> Single.error(Conversion.handleException(e)))
                    .subscribeOn(TPC.bestTPCScheduler()));
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return stopTracingWithException(e);
        }
    }

    @Override
    public CompletableFuture<? extends Result> batch(BatchType type, List<Object> queryOrIds, List<List<ByteBuffer>> values, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime)
    {
        try
        {
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

            final UUID tracingId = beginTraceBatch(internalState, internalOptions, customPayload, isTracingRequested);

            QueryHandler handler = ClientState.getCQLQueryHandler();

            Single<ResultMessage> resp = Single.defer(() ->
            {
                checkIsLoggedIn(internalState);

                List<Object> queryOrIdList = Conversion.toInternalQueryOrIds(queryOrIds);

                List<QueryHandler.Prepared> prepared = new ArrayList<>(queryOrIdList.size());
                for (int i = 0; i < queryOrIdList.size(); i++)
                {
                    Object query = queryOrIdList.get(i);
                    QueryHandler.Prepared p;
                    if (query instanceof String)
                    {
                        CQLStatement statement = QueryProcessor.parseStatement((String) query,
                                internalState.cloneWithKeyspaceIfSet(options.getKeyspace()));
                        p = new QueryHandler.Prepared(statement);
                    }
                    else
                    {
                        p = handler.getPrepared((org.apache.cassandra.utils.MD5Digest) query);
                        if (p == null)
                            throw new org.apache.cassandra.exceptions.PreparedQueryNotFoundException((org.apache.cassandra.utils.MD5Digest) query);
                    }

                    List<ByteBuffer> queryValues = values.get(i);
                    if (queryValues.size() != p.statement.getBindVariables().size())
                        throw new InvalidRequestException(String.format("There were %d markers(?) in CQL but %d bound variables",
                                p.statement.getBindVariables().size(),
                                queryValues.size()));

                    prepared.add(p);
                }

                BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(internalOptions, values, queryOrIdList);
                List<ModificationStatement> statements = new ArrayList<>(prepared.size());
                for (int i = 0; i < prepared.size(); i++)
                {
                    CQLStatement statement = prepared.get(i).statement;
                    batchOptions.prepareStatement(i, statement.getBindVariables());

                    if (!(statement instanceof ModificationStatement))
                        throw new InvalidRequestException("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");

                    statements.add((ModificationStatement) statement);
                }

                BatchStatement batch = BatchStatement.of(Conversion.toInternal(type), statements);

                return handler.processBatch(batch, internalState, batchOptions, null, queryStartNanoTime);
            });

            return Conversion.toFuture(resp
                    .map((result) -> Conversion.toResult(result, internalOptions.getProtocolVersion()).setTracingId(tracingId))
                    .flatMap(result -> Tracing.instance.stopSessionAsync().toSingleDefault(result))
                    .onErrorResumeNext((e) -> Single.error(Conversion.handleException(e)))
                    .subscribeOn(TPC.bestTPCScheduler()));
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return stopTracingWithException(e);
        }
    }

    @Override
    public boolean isInSchemaAgreement()
    {
        Map<String, List<String>> schemata = StorageProxy.describeSchemaVersions();
        return SchemaTool.isSchemaAgreement(schemata);
    }

    @Override
    public void captureClientWarnings()
    {
        ClientWarn.instance.captureWarnings();
    }

    @Override
    public List<String> getClientWarnings()
    {
        return ClientWarn.instance.getWarnings();
    }

    @Override
    public void resetClientWarnings()
    {
        ClientWarn.instance.resetWarnings();
    }

    private void checkIsLoggedIn(org.apache.cassandra.service.QueryState state)
    {
        if (!state.hasUser())
            throw new org.apache.cassandra.exceptions.UnauthorizedException("You have not logged in");
    }

    private CompletableFuture<Result> stopTracingWithException(Exception e)
    {
        CompletableFuture<Result> future = new CompletableFuture<>();
        Tracing.instance.stopSessionAsync().subscribe(() -> future.completeExceptionally(e));
        return future;
    }

    private UUID beginTraceQuery(String cql, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        if (!isTracingRequested)
            return null;

        final UUID sessionId = Tracing.instance.newSession(customPayload);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", cql);
        if (options.getPagingOptions() != null)
            builder.put("page_size", Integer.toString(options.getPagingOptions().pageSize().rawSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency(state) != null)
            builder.put("serial_consistency_level", options.getSerialConsistency(state).name());

        Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
        return sessionId;
    }

    private UUID beginTraceExecute(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        if (!isTracingRequested)
            return null;

        final UUID sessionId = Tracing.instance.newSession(customPayload);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (options.getPagingOptions() != null)
            builder.put("page_size", Integer.toString(options.getPagingOptions().pageSize().rawSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency(state) != null)
            builder.put("serial_consistency_level", options.getSerialConsistency(state).name());
        builder.put("query", statement.getQueryString());

        for (int i = 0; i < statement.getBindVariables().size(); i++)
        {
            ColumnSpecification cs = statement.getBindVariables().get(i);
            String boundName = cs.name.toString();
            String boundValue = cs.type.asCQL3Type().toCQLLiteral(options.getValues().get(i), options.getProtocolVersion());
            if (boundValue.length() > 1000)
                boundValue = boundValue.substring(0, 1000) + "...'";

            //Here we prefix boundName with the index to avoid possible collision in builder keys due to
            //having multiple boundValues for the same variable
            builder.put("bound_var_" + i + "_" + boundName, boundValue);
        }

        Tracing.instance.begin("Execute CQL3 prepared query", state.getClientAddress(), builder.build());
        return sessionId;
    }

    private UUID beginTracePrepare(String cql, org.apache.cassandra.service.QueryState state, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        if (!isTracingRequested)
            return null;

        final UUID sessionId = Tracing.instance.newSession(customPayload);

        Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", cql));
        return sessionId;
    }

    private UUID beginTraceBatch(org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        if (!isTracingRequested)
            return null;

        final UUID sessionId = Tracing.instance.newSession(customPayload);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency(state) != null)
            builder.put("serial_consistency_level", options.getSerialConsistency(state).name());

        // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
        Tracing.instance.begin("Execute batch of CQL3 queries", state.getClientAddress(), builder.build());
        return sessionId;

    }

    private Single<Result> processStatement(CQLStatement statement,
                                            QueryState state, QueryOptions options,
                                            Map<String, ByteBuffer> customPayload, long queryStartNanoTime,
                                            UUID tracingId)
    {
        Single<Result> resp = interceptor.interceptQuery(handler, statement, state, options, customPayload, queryStartNanoTime);
        if (resp == null)
        {
            org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
            org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

            resp = handler
                    .processStatement(statement, internalState, internalOptions, customPayload, queryStartNanoTime)
                    .map(r -> Conversion.toResult(r, internalOptions.getProtocolVersion()));
        }

        return resp
                .map(r -> r.setTracingId(tracingId))
                .flatMap(result -> Tracing.instance.stopSessionAsync().toSingleDefault(result))
                .onErrorResumeNext((e) -> Single.error(Conversion.handleException(e)))
                .subscribeOn(TPC.bestTPCScheduler());
    }

    /**
     * When "cassandra.join_ring" is "false" {@link StorageService#initServer()}  will not wait for schema to propagate to the coordinator only node. This
     * method fixes that limitation by waiting for at least one backend ring member to become available and for their schemas to agree before allowing
     * initialization to continue.
     */
    private void waitForSchema(int delay)
    {
        for (int i = 0; i < delay; i += 1000)
        {
            if (Gossiper.instance.getLiveTokenOwners().size() > 0 && isInSchemaAgreement())
            {
                logger.debug("current schema version: {}", SchemaManager.instance.getVersion());
                break;
            }

            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }
}
