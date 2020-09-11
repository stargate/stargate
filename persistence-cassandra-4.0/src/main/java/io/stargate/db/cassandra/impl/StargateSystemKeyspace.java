package io.stargate.db.cassandra.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import io.stargate.db.Result;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

public class StargateSystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(StargateSystemKeyspace.class);

    public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
    public static final String LOCAL_TABLE_NAME = "stargate_local";
    public static final String PEERS_TABLE_NAME = "stargate_peers";
    public static final String PEERS_V2_TABLE_NAME = "stargate_peers_v2";

    public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

    private static final TableMetadata Local =
            parse(LOCAL_TABLE_NAME,
                    "information about the local node",
                    "CREATE TABLE %s ("
                            + "key text,"
                            + "bootstrapped text,"
                            + "broadcast_address inet,"
                            + "broadcast_port int,"
                            + "cluster_name text,"
                            + "cql_version text,"
                            + "data_center text,"
                            + "gossip_generation int,"
                            + "host_id uuid,"
                            + "listen_address inet,"
                            + "listen_port int,"
                            + "native_protocol_version text,"
                            + "partitioner text,"
                            + "rack text,"
                            + "release_version text,"
                            + "rpc_address inet,"
                            + "rpc_port int,"
                            + "schema_version uuid,"
                            + "tokens set<varchar>,"
                            + "truncated_at map<uuid, blob>,"
                            + "PRIMARY KEY ((key)))"
            ).recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance)
                    .build();

    private static final TableMetadata Peers =
            parse(PEERS_TABLE_NAME,
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
                            + "PRIMARY KEY ((peer)))")
                    .build();

    private static final TableMetadata PeersV2 =
            parse(PEERS_V2_TABLE_NAME,
                    "information about known peers in the cluster",
                    "CREATE TABLE %s ("
                            + "peer inet,"
                            + "peer_port int,"
                            + "data_center text,"
                            + "host_id uuid,"
                            + "preferred_ip inet,"
                            + "preferred_port int,"
                            + "rack text,"
                            + "release_version text,"
                            + "native_address inet,"
                            + "native_port int,"
                            + "schema_version uuid,"
                            + "tokens set<varchar>,"
                            + "PRIMARY KEY ((peer), peer_port))")
                    .build();

    private static TableMetadata.Builder parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SYSTEM_KEYSPACE_NAME)
                .id(TableId.forSystemTable(SYSTEM_KEYSPACE_NAME, table))
                .gcGraceSeconds(0)
                .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                .comment(description);
    }

    public static Tables tables()
    {
        return Tables.of(Local, Peers, PeersV2);
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
                "native_protocol_version," +
                "data_center," +
                "rack," +
                "partitioner," +
                "rpc_address," +
                "rpc_port," +
                "broadcast_address," +
                "broadcast_port," +
                "listen_address," +
                "listen_port," +
                "bootstrapped," +
                "host_id," +
                "tokens," +
                "schema_version" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(format(req, SYSTEM_KEYSPACE_NAME, LOCAL_TABLE_NAME),
                SystemKeyspace.LOCAL,
                DatabaseDescriptor.getClusterName(),
                FBUtilities.getReleaseVersionString(),
                QueryProcessor.CQL_VERSION.toString(),
                String.valueOf(ProtocolVersion.CURRENT.asInt()),
                snitch.getLocalDatacenter(),
                snitch.getLocalRack(),
                DatabaseDescriptor.getPartitioner().getClass().getName(),
                DatabaseDescriptor.getRpcAddress(),
                DatabaseDescriptor.getNativeTransportPort(),
                FBUtilities.getJustBroadcastAddress(),
                DatabaseDescriptor.getStoragePort(),
                FBUtilities.getJustLocalAddress(),
                DatabaseDescriptor.getStoragePort(),
                SystemKeyspace.BootstrapState.COMPLETED.name(),
                SystemKeyspace.getLocalHostId(),
                Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken().toString()),
                SCHEMA_VERSION);
    }

    public IEndpointStateChangeSubscriber getPeersUpdater()
    {
        return new PeersUpdater();
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

    private static boolean isSystemPeers(SelectStatement statement)
    {
        return statement.columnFamily().equals(SystemKeyspace.LEGACY_PEERS);
    }

    private static boolean isSystemPeersV2(SelectStatement statement)
    {
        return statement.columnFamily().equals(SystemKeyspace.PEERS_V2);
    }

    private static boolean isSystemLocalOrPeers(CQLStatement statement)
    {
        if (statement instanceof SelectStatement)
        {
            SelectStatement selectStatement = (SelectStatement) statement;
            return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) &&
                    (selectStatement.columnFamily().equals(SystemKeyspace.LOCAL) || isSystemPeers(selectStatement) || isSystemPeersV2(selectStatement));
        }
        return false;
    }

    private static ResultMessage.Rows interceptSystemLocalOrPeers(CQLStatement statement, org.apache.cassandra.service.QueryState state, org.apache.cassandra.cql3.QueryOptions options, long queryStartNanoTime)
    {
        SelectStatement selectStatement = (SelectStatement) statement;
        TableMetadata tableMetadata = Local;
        if (isSystemPeers(selectStatement))
            tableMetadata = Peers;
        else if (isSystemPeersV2(selectStatement))
            tableMetadata = PeersV2;
        SelectStatement peersSelectStatement =
                new SelectStatement(tableMetadata,
                        selectStatement.bindVariables,
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

    public static synchronized void updatePeerInfo(InetAddressAndPort ep, String columnName, Object value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        String req = "INSERT INTO %s.%s (peer, %s) VALUES (?, ?)";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME, columnName), ep.address, value);
        //This column doesn't match across the two tables
        if (columnName.equals("rpc_address"))
        {
            columnName = "native_address";
        }
        req = "INSERT INTO %s.%s (peer, peer_port, %s) VALUES (?, ?, ?)";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME, columnName), ep.address, ep.port, value);
    }

    public static synchronized void updatePeerNativeAddress(InetAddressAndPort ep, InetAddressAndPort address)
    {
        if (ep.equals(FBUtilities.getBroadcastAddressAndPort()))
            return;

        String req = "INSERT INTO %s.%s (peer, rpc_address) VALUES (?, ?)";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME), ep.address, address.address);
        req = "INSERT INTO %s.%s (peer, peer_port, native_address, native_port) VALUES (?, ?, ?, ?)";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME), ep.address, ep.port, address.address, address.port);
    }

    public static synchronized void removeEndpoint(InetAddressAndPort ep)
    {
        String req = "DELETE FROM %s.%s WHERE peer = ?";
        executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME), ep.address);
        req = String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",  SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME);
        executeInternal(req, ep.address, ep.port);
        forceBlockingFlush(PEERS_TABLE_NAME, PEERS_V2_TABLE_NAME);
    }

    public static void forceBlockingFlush(String ...cfnames)
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
        {
            List<ListenableFuture<CommitLogPosition>> futures = new ArrayList<>();

            for (String cfname : cfnames)
            {
                futures.add(Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(cfname).forceFlush());
            }
            FBUtilities.waitOnFutures(futures);
        }
    }

    public static class PeersUpdater implements IEndpointStateChangeSubscriber
    {
        @Override
        public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
        {
            for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
            {
                onChange(endpoint, entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
        }

        @Override
        public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
        {
            if (state == ApplicationState.STATUS || state == ApplicationState.STATUS_WITH_PORT)
            {
                return;
            }

            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState))
            {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            if (FBUtilities.getLocalAddressAndPort().equals(endpoint) || StorageService.instance.getTokenMetadata().isMember(endpoint))  // We only want stargate nodes (no members)
            {
                return;
            }

            switch (state)
            {
                case RELEASE_VERSION:
                    updatePeerInfo(endpoint, "release_version", value.value);
                    break;
                case DC:
                    updatePeerInfo(endpoint, "data_center", value.value);
                    break;
                case RACK:
                    updatePeerInfo(endpoint, "rack", value.value);
                    break;
                case RPC_ADDRESS:
                    try
                    {
                        updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case NATIVE_ADDRESS_AND_PORT:
                    try
                    {
                        InetAddressAndPort address = InetAddressAndPort.getByName(value.value);
                        updatePeerNativeAddress(endpoint, address);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case SCHEMA:
                    // Use a fix schema version for all peers (always in agreement) because stargate waits
                    // for DDL queries to reach agreement before returning.
                    updatePeerInfo(endpoint, "schema_version", Schema.instance.getVersion());
                    break;
                case HOST_ID:
                    updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value));
                    break;
            }

            updatePeerInfo(endpoint, "tokens", Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken().toString()));
        }

        @Override
        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
        }

        @Override
        public void onDead(InetAddressAndPort endpoint, EndpointState state)
        {
        }

        @Override
        public void onRemove(InetAddressAndPort endpoint)
        {
            removeEndpoint(endpoint);
        }

        @Override
        public void onRestart(InetAddressAndPort endpoint, EndpointState state)
        {
        }
    }
}
