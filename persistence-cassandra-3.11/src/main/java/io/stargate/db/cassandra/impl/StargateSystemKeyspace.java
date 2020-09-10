package io.stargate.db.cassandra.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.cassandraConstants;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import io.stargate.db.Result;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

public class StargateSystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(StargateSystemKeyspace.class);

    public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
    public static final String LOCAL_TABLE_NAME = "stargate_local";
    public static final String PEERS_TABLE_NAME = "stargate_peers";

    public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

    private static final CFMetaData Local =
            compile(LOCAL_TABLE_NAME,
                    "information about the local node",
                    "CREATE TABLE %s ("
                            + "key text,"
                            + "bootstrapped text,"
                            + "broadcast_address inet,"
                            + "cluster_name text,"
                            + "cql_version text,"
                            + "data_center text,"
                            + "gossip_generation int,"
                            + "host_id uuid,"
                            + "listen_address inet,"
                            + "native_protocol_version text,"
                            + "partitioner text,"
                            + "rack text,"
                            + "release_version text,"
                            + "rpc_address inet,"
                            + "schema_version uuid,"
                            + "thrift_version text,"
                            + "tokens set<varchar>,"
                            + "truncated_at map<uuid, blob>,"
                            + "PRIMARY KEY ((key)))");

    public static final CFMetaData Peers =
            compile(PEERS_TABLE_NAME,
                    "information about known peers in the cluster",
                    "CREATE TABLE %s ("
                            + "peer inet,"
                            + "data_center text,"
                            + "host_id uuid,"
                            + "preferred_ip inet,"
                            + "rack text,"
                            + "release_version text,"
                            + "rpc_address inet,"
                            + "schema_version uuid,"
                            + "tokens set<varchar>,"
                            + "PRIMARY KEY ((peer)))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SYSTEM_KEYSPACE_NAME)
                .comment(description);
    }

    public static Tables tables()
    {
        return Tables.of(Local, Peers);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SYSTEM_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), Functions.none());
    }

    public static void persistLocalMetadata()
    {
        String req = "INSERT INTO %s.%s (" +
                "key," +
                "cluster_name," +
                "release_version," +
                "cql_version," +
                "thrift_version," +
                "native_protocol_version," +
                "data_center," +
                "rack," +
                "partitioner," +
                "rpc_address," +
                "broadcast_address," +
                "listen_address," +
                "bootstrapped," +
                "host_id," +
                "tokens," +
                "schema_version" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(String.format(req, SYSTEM_KEYSPACE_NAME, LOCAL_TABLE_NAME),
                SystemKeyspace.LOCAL,
                DatabaseDescriptor.getClusterName(),
                FBUtilities.getReleaseVersionString(),
                QueryProcessor.CQL_VERSION.toString(),
                cassandraConstants.VERSION,
                String.valueOf(ProtocolVersion.CURRENT.asInt()),
                snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                snitch.getRack(FBUtilities.getBroadcastAddress()),
                DatabaseDescriptor.getPartitioner().getClass().getName(),
                DatabaseDescriptor.getRpcAddress(),
                FBUtilities.getBroadcastAddress(),
                FBUtilities.getLocalAddress(),
                SystemKeyspace.BootstrapState.COMPLETED.name(),
                SystemKeyspace.getLocalHostId(),
                Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken().toString()),
                SCHEMA_VERSION);
    }

    private static boolean isSystemLocal(SelectStatement statement)
    {
        return statement.columnFamily().equals(SystemKeyspace.LOCAL);
    }

    private static boolean isSystemLocalOrPeers(CQLStatement statement)
    {
        if (statement instanceof SelectStatement)
        {
            SelectStatement selectStatement = (SelectStatement) statement;
            return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) && (isSystemLocal(selectStatement) || selectStatement.columnFamily().equals(SystemKeyspace.PEERS)) ;
        }
        return false;
    }

    private static ResultMessage.Rows interceptSystemLocalOrPeers(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, long queryStartNanoTime)
    {
        SelectStatement selectStatement = (SelectStatement) statement;
        SelectStatement peersSelectStatement =
                new SelectStatement(isSystemLocal(selectStatement) ? Local : Peers,
                        selectStatement.getBoundTerms(),
                        selectStatement.parameters,
                        selectStatement.getSelection(),
                        selectStatement.getRestrictions(),
                        false,
                        null,
                        null,
                        null,
                        null);
        ResultMessage.Rows rows = peersSelectStatement.execute(state, options, queryStartNanoTime);
        return new ResultMessage.Rows(new ResultSet(selectStatement.getResultMetadata(), rows.result.rows));
    }


    public static boolean maybeCompleteSystemLocalOrPeers(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, long queryStartNanoTime, CompletableFuture<Result> future)
    {
        if (isSystemLocalOrPeers(statement))
        {
            future.complete(Conversion.toResult(interceptSystemLocalOrPeers(statement, state, options, queryStartNanoTime), options.getProtocolVersion()));
            return true;
        }
        return false;
    }

    public static boolean maybeCompleteSystemLocalOrPeersInternal(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, long queryStartNanoTime, CompletableFuture<ResultMessage> future)
    {
        if (isSystemLocalOrPeers(statement))
        {
            future.complete(interceptSystemLocalOrPeers(statement, state, options, queryStartNanoTime));
            return true;
        }
        return false;
    }

    public static Future<?> updatePeerInfo(final InetAddress ep, final String columnName, final Object value, ExecutorService executorService)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            return Futures.immediateFuture(null);
        }

        String req = "INSERT INTO %s.%s (peer, %s) VALUES (?, ?)";
        return executorService.submit((Runnable) () -> executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME, columnName), ep, value));
    }

    public static void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM %s.%s WHERE peer = ?";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME), ep);
        forceBlockingFlush(PEERS_TABLE_NAME);
    }

    public static void forceBlockingFlush(String cfname)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
        {
            FBUtilities.waitOnFuture(Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(cfname).forceFlush());
        }
    }

    public static class PeersUpdater implements IEndpointStateChangeSubscriber
    {
        @Override
        public void onJoin(InetAddress endpoint, EndpointState epState)
        {
            for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
            {
                onChange(endpoint, entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
        }

        @Override
        public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
        {
            if (state == ApplicationState.STATUS)
            {
                return;
            }

            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState))
            {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            if (StorageService.instance.getTokenMetadata().isMember(endpoint))  // We only want stargate nodes (no members)
            {
                return;
            }

            final ExecutorService executor = StageManager.getStage(Stage.MUTATION);
            switch (state)
            {
                case RELEASE_VERSION:
                    updatePeerInfo(endpoint, "release_version", value.value, executor);
                    break;
                case DC:
                    updatePeerInfo(endpoint, "data_center", value.value, executor);
                    break;
                case RACK:
                    updatePeerInfo(endpoint, "rack", value.value, executor);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value), executor);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case SCHEMA:
                    // Use a fix schema version for all peers (always in agreement) because stargate waits
                    // for DDL queries to reach agreement before returning.
                    updatePeerInfo(endpoint, "schema_version", SCHEMA_VERSION, executor);

                    // This fix schedules a schema pull for the non-member node and is required because
                    // `StorageService.onChange()` doesn't do this for non-member nodes.
                    MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
                    break;
                case HOST_ID:
                    updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value), executor);
                    break;
            }

            updatePeerInfo(endpoint, "tokens", Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken().toString()), executor);
        }

        @Override
        public void onAlive(InetAddress endpoint, EndpointState state)
        {
        }

        @Override
        public void onDead(InetAddress endpoint, EndpointState state)
        {
        }

        @Override
        public void onRemove(InetAddress endpoint)
        {
            removeEndpoint(endpoint);
        }

        @Override
        public void onRestart(InetAddress endpoint, EndpointState state)
        {
        }
    }
}
