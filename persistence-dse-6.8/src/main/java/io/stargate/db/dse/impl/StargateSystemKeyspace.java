package io.stargate.db.dse.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.nodes.Nodes;
import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;
import com.datastax.bdp.db.util.ProductVersion;
import com.datastax.bdp.gms.DseState;

public class StargateSystemKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(StargateSystemKeyspace.class);

    public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
    public static final String LOCAL_TABLE_NAME = "local";
    public static final String PEERS_TABLE_NAME = "peers";
    public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

    public final static StargateSystemKeyspace instance = new StargateSystemKeyspace();

    private final ConcurrentMap<InetAddress, StargatePeerInfo> peers = new ConcurrentHashMap<>();
    private final StargateLocalInfo local = new StargateLocalInfo();

    public Map<InetAddress, StargatePeerInfo> getPeers()
    {
        return peers;
    }

    public StargateLocalInfo getLocal()
    {
        return local;
    }

    public void persistLocalMetadata()
    {
        local.setClusterName(DatabaseDescriptor.getClusterName());
        local.setReleaseVersion(ProductVersion.getReleaseVersion().toString());
        local.setDseVersion(ProductVersion.getDSEVersion().toString());
        local.setCqlVersion(QueryProcessor.CQL_VERSION);
        local.setNativeProtocolVersion(String.valueOf(ProtocolVersion.CURRENT.asInt()));
        local.setDataCenter(DatabaseDescriptor.getLocalDataCenter());
        local.setRack(DatabaseDescriptor.getLocalRack());
        local.setPartitioner(DatabaseDescriptor.getPartitioner().getClass().getName());
        local.setBroadcastAddress(FBUtilities.getBroadcastAddress());
        local.setListenAddress(FBUtilities.getLocalAddress());
        local.setNativeAddress(DatabaseDescriptor.getNativeTransportAddress());
        local.setNativePort(DatabaseDescriptor.getNativeTransportPort());
        local.setNativePortSsl(DatabaseDescriptor.getNativeTransportPortSSL());
        local.setStoragePort(DatabaseDescriptor.getStoragePort());
        local.setStoragePortSsl(DatabaseDescriptor.getSSLStoragePort());
        local.setJmxPort(DatabaseDescriptor.getJMXPort().orElse(null));
        local.setHostId(Nodes.local().get().getHostId());
    }

    public static boolean isSystemLocal(SelectStatement statement)
    {
        return statement.table().equals(LocalNodeSystemView.NAME);
    }

    public static boolean isSystemLocalOrPeers(CQLStatement statement)
    {
        if (statement instanceof SelectStatement)
        {
            SelectStatement selectStatement = (SelectStatement) statement;
            return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_VIEWS_KEYSPACE_NAME) &&
                    (isSystemLocal(selectStatement) || selectStatement.table().equals(PeersSystemView.NAME));
        }
        return false;
    }

    public static boolean isStargateNode(EndpointState epState)
    {
        VersionedValue value = epState.getApplicationState(ApplicationState.X10);
        return value != null && value.value.equals("stargate");
    }

    public IEndpointStateChangeSubscriber getPeersUpdater()
    {
        return new PeersUpdater();
    }

    public static void initialize()
    {
        VirtualKeyspace.Builder builder = VirtualKeyspace.newBuilder(SYSTEM_KEYSPACE_NAME)
                .addView(new StargatePeersView())
                .addView(new StargateLocalView());
        SchemaManager.instance.load(builder.build());
    }

    private class PeersUpdater implements IEndpointStateChangeSubscriber
    {
        @Override
        public void onJoin(InetAddress endpoint, EndpointState epState)
        {
            if (!isStargateNode(epState))
                return;

            for (Map.Entry<ApplicationState, VersionedValue> entry : epState.states())
            {
                applyState(endpoint, entry.getKey(), entry.getValue(), epState);
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

            // We only want stargate nodes
            if (FBUtilities.getLocalAddress().equals(endpoint) || !isStargateNode(epState))
            {
                return;
            }

            if (peers.containsKey(endpoint))
                applyState(endpoint, state, value, epState);
            else
                onJoin(endpoint, epState);
        }

        private void updateDseState(VersionedValue value, InetAddress endpoint, StargatePeerInfo peer)
        {
            Map<String, Object> dseState = DseState.getValues(value);
            ProductVersion.Version version = DseState.getDseVersion(dseState);

            if (version != null && (peer == null || peer.getDseVersion() == null || !peer.getDseVersion().equals(version)))
            {
                peer.setDseVersion(version.toString());
            }
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
            peers.remove(endpoint);
        }

        @Override
        public void onRestart(InetAddress endpoint, EndpointState state)
        {
        }

        private void applyState(InetAddress endpoint, ApplicationState state, VersionedValue value, EndpointState epState)
        {
            StargatePeerInfo peer = peers.computeIfAbsent(endpoint, StargatePeerInfo::new);

            switch (state)
            {
                case RELEASE_VERSION:
                    peer.setReleaseVersion(value.value);
                    break;
                case DC:
                    peer.setDataCenter(value.value);
                    break;
                case RACK:
                    peer.setRack(value.value);
                    break;
                case NATIVE_TRANSPORT_ADDRESS:
                    try
                    {
                        peer.setNativeAddress(InetAddress.getByName(value.value));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                    break;
                case NATIVE_TRANSPORT_PORT:
                    peer.setNativePort(Integer.parseInt(value.value));
                    break;
                case NATIVE_TRANSPORT_PORT_SSL:
                    peer.setNativePortSsl(Integer.parseInt(value.value));
                    break;
                case SCHEMA:
                    // This fix schedules a schema pull for the non-member node and is required because
                    // `StorageService.onChange()` doesn't do this for non-member nodes.
                    MigrationManager.instance.scheduleSchemaPull(endpoint, epState, String.format("gossip schema version change to %s", value.value));
                    break;
                case STORAGE_PORT:
                    peer.setStoragePort(Integer.parseInt(value.value));
                    break;
                case STORAGE_PORT_SSL:
                    peer.setStoragePortSsl(Integer.parseInt(value.value));
                    break;
                case JMX_PORT:
                    peer.setJmxPort(Integer.parseInt(value.value));
                    break;
                case HOST_ID:
                    peer.setHostId(UUID.fromString(value.value));
                    break;
                case DSE_GOSSIP_STATE:
                    updateDseState(value, endpoint, peer);
                    break;
            }
        }
    }

    public static TableMetadata virtualFromLegacy(TableMetadata legacy, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(SYSTEM_KEYSPACE_NAME, table)
                .kind(TableMetadata.Kind.VIRTUAL);
        legacy.partitionKeyColumns().forEach(cm -> builder.addPartitionKeyColumn(cm.name, cm.type));
        legacy.staticColumns().forEach(cm -> builder.addStaticColumn(cm.name, cm.type.freeze()));
        legacy.clusteringColumns().forEach(cm -> builder.addClusteringColumn(cm.name, cm.type));
        legacy.regularColumns().forEach(cm -> builder.addRegularColumn(cm.name, cm.type.freeze()));
        return builder.build();
    }
}
