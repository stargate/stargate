package io.stargate.db.cassandra.impl;

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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
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

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.stargate.db.Authenticator;
import io.stargate.db.BatchType;
import io.stargate.db.ClientState;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.cassandra.datastore.InternalDataStore;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.common.util.SchemaTool;

import static io.stargate.db.cassandra.datastore.InternalDataStore.EXECUTOR;

public class CassandraPersistence implements Persistence<Config, org.apache.cassandra.service.ClientState, org.apache.cassandra.service.QueryState>
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraPersistence.class);

    private DataStore root;
    private CassandraDaemon daemon;
    private Authenticator authenticator;
    private QueryHandler handler;

    @Override
    public String name()
    {
        return "Apache Cassandra";
    }

    @Override
    public void initialize(Config config)
    {
        logger.info("Initializing CassandraPersistence");
        System.setProperty("cassandra.join_ring", "false");
        daemon = new CassandraDaemon(true);

        DatabaseDescriptor.daemonInitialization(() -> config);
        try
        {
            daemon.init(null);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to start Cassandra persistence layer", e);
        }

        Schema.instance.load(StargateSystemKeyspace.metadata());

        daemon.start();

        waitForSchema(StorageService.RING_DELAY);

        root = new InternalDataStore();
        authenticator = new AuthenticatorWrapper(DatabaseDescriptor.getAuthenticator());
        handler = org.apache.cassandra.service.ClientState.getCQLQueryHandler();

        Gossiper.instance.register(new StargateSystemKeyspace.PeersUpdater());
        StargateSystemKeyspace.persistLocalMetadata();

        StorageService.instance.waitForSchema(StorageService.RING_DELAY);
    }

    @Override
    public void destroy()
    {
        if (daemon != null)
        {
            root = null;
            daemon.deactivate();
            daemon = null;
        }
    }

    @Override
    public void registerEventListener(EventListener listener)
    {
        EventListenerWrapper wrapper = new EventListenerWrapper(listener);
        StorageService.instance.register(wrapper);
        MigrationManager.instance.register(wrapper);
    }

    @Override
    public boolean isRpcReady(InetAddressAndPort endpoint)
    {
        return StorageService.instance.isRpcReady(Conversion.toInternal(endpoint));
    }

    @Override
    public InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint)
    {
        try
        {
            return InetAddressAndPort.getByName(StorageService.instance.getRpcaddress(Conversion.toInternal(endpoint)));
        }
        catch (UnknownHostException e)
        {
            // That should not happen, so log an error, but return the
            // endpoint address since there's a good change this is right
            logger.error("Problem retrieving RPC address for {}", endpoint, e);
            return InetAddressAndPort.getByAddressOverrideDefaults(endpoint.address, DatabaseDescriptor.getNativeTransportPort());
        }
    }


    @Override
    public DataStore newDataStore(QueryState<org.apache.cassandra.service.QueryState> state, QueryOptions<org.apache.cassandra.service.ClientState> queryOptions)
    {
        return new InternalDataStore(root, Conversion.toInternal(state), Conversion.toInternal(queryOptions));
    }

    @Override
    public QueryState newQueryState(ClientState clientState)
    {
        return new QueryStateWrapper(clientState);
    }

    @Override
    public ClientState<org.apache.cassandra.service.ClientState> newClientState(SocketAddress remoteAddress, InetSocketAddress publicAddress)
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
        ClientStateWrapper state = ClientStateWrapper.forExternalCalls((InetSocketAddress) remoteAddress, publicAddress);
        state.login(new AuthenticatorWrapper.AuthenticatedUserWrapper(AuthenticatedUser.ANONYMOUS_USER));
        return state;
    }

    @Override
    public ClientState newClientState(String name)
    {
        if (Strings.isNullOrEmpty(name))
        {
            return ClientStateWrapper.forInternalCalls();
        }

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
        CompletableFuture<Result> future = new CompletableFuture<>();

        EXECUTOR.submit(() ->
        {
            try
            {
                org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
                org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

                UUID tracingId = null;
                if (isTracingRequested)
                {
                    tracingId = UUIDGen.getTimeUUID();
                    internalState.prepareTracingSession(tracingId);
                }

                if (internalState.traceNextQuery())
                {
                    internalState.createTracingSession(customPayload);
                    beginTraceQuery(cql, state, options);
                }

                CQLStatement statement = QueryProcessor.parseStatement(cql, internalState).statement;
                if (!StargateSystemKeyspace.maybeCompleteSystemLocalOrPeers(statement, internalState, internalOptions, queryStartNanoTime, future))
                {
                    future.complete(Conversion.toResult(
                            QueryProcessor.instance
                                    .processStatement(statement, internalState, internalOptions, queryStartNanoTime), internalOptions.getProtocolVersion())
                            .setTracingId(tracingId));
                }
            }
            catch (Throwable t)
            {
                Conversion.handleException(future, t);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<? extends Result> execute(MD5Digest id, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime)
    {
        CompletableFuture<Result> future = new CompletableFuture<>();

        EXECUTOR.submit(() ->
        {
            try
            {
                org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);
                org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);

                UUID tracingId = null;
                if (isTracingRequested)
                {
                    tracingId = UUIDGen.getTimeUUID();
                    internalState.prepareTracingSession(tracingId);
                }

                ParsedStatement.Prepared prepared = handler.getPrepared(Conversion.toInternal(id));

                if (prepared == null)
                {
                    throw new PreparedQueryNotFoundException(id);
                }

                if (internalState.traceNextQuery())
                {
                    internalState.createTracingSession(customPayload);
                    beginTraceExecute(prepared, state, options, internalOptions.getProtocolVersion());
                }

                CQLStatement statement = prepared.statement;
                if (!StargateSystemKeyspace.maybeCompleteSystemLocalOrPeers(statement, internalState, internalOptions, queryStartNanoTime, future))
                {
                    future.complete(Conversion.toResult(
                            handler
                                    .processPrepared(statement, internalState, internalOptions, customPayload, queryStartNanoTime), internalOptions.getProtocolVersion())
                            .setTracingId(tracingId));
                }
            }
            catch (Throwable t)
            {
                Conversion.handleException(future, t);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<? extends Result> prepare(String cql, QueryState state, Map<String, ByteBuffer> customPayload, boolean isTracingRequested)
    {
        CompletableFuture<Result> future = new CompletableFuture<>();

        InternalDataStore.EXECUTOR.submit(() ->
        {
            try
            {
                org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

                UUID tracingId = null;
                if (isTracingRequested)
                {
                    tracingId = UUIDGen.getTimeUUID();
                    internalState.prepareTracingSession(tracingId);
                }

                if (internalState.traceNextQuery())
                {
                    internalState.createTracingSession(customPayload);
                    beginTracePrepare(cql, state);
                }

                future.complete(Conversion.toPrepared(handler.prepare(cql, internalState, customPayload))
                        .setTracingId(tracingId));
            }
            catch (Throwable t)
            {
                Conversion.handleException(future, t);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        });

        return future;
    }

    @Override
    public CompletableFuture<? extends Result> batch(BatchType type, List<Object> queryOrIds, List<List<ByteBuffer>> values, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, boolean isTracingRequested, long queryStartNanoTime)
    {
        CompletableFuture<Result> future = new CompletableFuture<>();

        EXECUTOR.submit(() ->
        {
            try
            {
                org.apache.cassandra.cql3.QueryOptions internalOptions = Conversion.toInternal(options);
                org.apache.cassandra.service.QueryState internalState = Conversion.toInternal(state);

                UUID tracingId = null;
                if (isTracingRequested)
                {
                    tracingId = UUIDGen.getTimeUUID();
                    internalState.prepareTracingSession(tracingId);
                }

                if (internalState.traceNextQuery())
                {
                    internalState.createTracingSession(customPayload);
                    beginTraceBatch(state, options);
                }

                List<Object> queryOrIdList = Conversion.toInternalQueryOrIds(queryOrIds);
                BatchStatement.Type internalBatchType = Conversion.toInternal(type);

                int boundTerms = 0;
                List<ParsedStatement.Prepared> prepared = new ArrayList<>(queryOrIdList.size());
                for (int i = 0; i < queryOrIdList.size(); i++)
                {
                    Object query = queryOrIdList.get(i);
                    ParsedStatement.Prepared p;
                    if (query instanceof String)
                    {
                        org.apache.cassandra.service.ClientState cs = internalState.getClientState();
                        if (options.getKeyspace() != null)
                        {
                            cs.setKeyspace(options.getKeyspace());
                        }

                        p = QueryProcessor.parseStatement((String) query, internalState);
                    }
                    else
                    {
                        p = handler.getPrepared((org.apache.cassandra.utils.MD5Digest) query);
                        if (null == p)
                        {
                            throw new PreparedQueryNotFoundException((MD5Digest) query);
                        }
                    }

                    List<ByteBuffer> queryValues = values.get(i);
                    if (queryValues.size() != p.statement.getBoundTerms())
                    {
                        throw new InvalidRequestException(String.format("There were %d markers(?) in CQL but %d bound variables",
                                p.statement.getBoundTerms(),
                                queryValues.size()));
                    }
                    boundTerms = p.statement.getBoundTerms();
                    prepared.add(p);
                }

                BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(internalOptions, values, queryOrIdList);
                List<ModificationStatement> statements = new ArrayList<>(prepared.size());
                for (int i = 0; i < prepared.size(); i++)
                {
                    ParsedStatement.Prepared p = prepared.get(i);
                    batchOptions.prepareStatement(i, p.boundNames);

                    if (!(p.statement instanceof ModificationStatement))
                    {
                        throw new InvalidRequestException("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
                    }

                    statements.add((ModificationStatement) p.statement);
                }

                // Note: It's ok at this point to pass a bogus value for the number of bound terms in the BatchState ctor
                // (and no value would be really correct, so we prefer passing a clearly wrong one).
                BatchStatement batch = new BatchStatement(boundTerms, internalBatchType, statements, Attributes.none());

                ResultMessage response = handler.processBatch(batch, internalState, batchOptions, customPayload, queryStartNanoTime);


                future.complete(Conversion.toResult(response, internalOptions.getProtocolVersion())
                        .setTracingId(tracingId));
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                Conversion.handleException(future, e);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        });

        return future;
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

    private void beginTraceQuery(String cql, QueryState state, QueryOptions options)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", cql);
        if (options.getPageSize() > 0)
        {
            builder.put("page_size", Integer.toString(options.getPageSize()));
        }
        if (options.getConsistency() != null)
        {
            builder.put("consistency_level", options.getConsistency().name());
        }
        if (options.getSerialConsistency() != null)
        {
            builder.put("serial_consistency_level", options.getSerialConsistency().name());
        }

        Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
    }

    private void beginTraceExecute(ParsedStatement.Prepared prepared, QueryState state, QueryOptions options, ProtocolVersion version)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (options.getPageSize() > 0)
        {
            builder.put("page_size", Integer.toString(options.getPageSize()));
        }
        if (options.getConsistency() != null)
        {
            builder.put("consistency_level", options.getConsistency().name());
        }
        if (options.getSerialConsistency() != null)
        {
            builder.put("serial_consistency_level", options.getSerialConsistency().name());
        }
        builder.put("query", prepared.rawCQLStatement);

        for (int i = 0; i < prepared.boundNames.size(); i++)
        {
            ColumnSpecification cs = prepared.boundNames.get(i);
            String boundName = cs.name.toString();
            List<ByteBuffer> values = options.getValues();
            String boundValue = cs.type.asCQL3Type().toCQLLiteral(values.get(i), version);
            if (boundValue.length() > 1000)
            {
                boundValue = boundValue.substring(0, 1000) + "...'";
            }

            //Here we prefix boundName with the index to avoid possible collission in builder keys due to
            //having multiple boundValues for the same variable
            builder.put("bound_var_" + Integer.toString(i) + "_" + boundName, boundValue);
        }

        Tracing.instance.begin("Execute CQL3 prepared query", state.getClientAddress(), builder.build());
    }

    private void beginTracePrepare(String cql, QueryState state)
    {
        Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", cql));
    }

    private void beginTraceBatch(QueryState state, QueryOptions options)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (options.getConsistency() != null)
        {
            builder.put("consistency_level", options.getConsistency().name());
        }
        if (options.getSerialConsistency() != null)
        {
            builder.put("serial_consistency_level", options.getSerialConsistency().name());
        }

        // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
        Tracing.instance.begin("Execute batch of CQL3 queries", state.getClientAddress(), builder.build());
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
                logger.debug("current schema version: {}", Schema.instance.getVersion());
                break;
            }

            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }
}
