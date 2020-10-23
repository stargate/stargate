package io.stargate.db.dse.impl;

import com.datastax.bdp.db.nodes.Nodes;
import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;
import com.datastax.bdp.db.util.ProductVersion;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

public class StargateSystemKeyspace {
  public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
  public static final String LOCAL_TABLE_NAME = "local";
  public static final String PEERS_TABLE_NAME = "peers";
  public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

  public static final StargateSystemKeyspace instance = new StargateSystemKeyspace();

  private final ConcurrentMap<InetAddress, StargatePeerInfo> peers = new ConcurrentHashMap<>();
  private final StargateLocalInfo local = new StargateLocalInfo();

  public ConcurrentMap<InetAddress, StargatePeerInfo> getPeers() {
    return peers;
  }

  public StargateLocalInfo getLocal() {
    return local;
  }

  public void persistLocalMetadata() {
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

  public static boolean isSystemLocal(SelectStatement statement) {
    return statement.table().equals(LocalNodeSystemView.NAME);
  }

  public static boolean isSystemLocalOrPeers(CQLStatement statement) {
    if (statement instanceof SelectStatement) {
      SelectStatement selectStatement = (SelectStatement) statement;
      return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_VIEWS_KEYSPACE_NAME)
          && (isSystemLocal(selectStatement)
              || selectStatement.table().equals(PeersSystemView.NAME));
    }
    return false;
  }

  public static void initialize() {
    VirtualKeyspace.Builder builder =
        VirtualKeyspace.newBuilder(SYSTEM_KEYSPACE_NAME)
            .addView(new StargatePeersView())
            .addView(new StargateLocalView());
    SchemaManager.instance.load(builder.build());
  }

  public static TableMetadata virtualFromLegacy(TableMetadata legacy, String table) {
    TableMetadata.Builder builder =
        TableMetadata.builder(SYSTEM_KEYSPACE_NAME, table).kind(TableMetadata.Kind.VIRTUAL);
    legacy.partitionKeyColumns().forEach(cm -> builder.addPartitionKeyColumn(cm.name, cm.type));
    legacy.staticColumns().forEach(cm -> builder.addStaticColumn(cm.name, cm.type.freeze()));
    legacy.clusteringColumns().forEach(cm -> builder.addClusteringColumn(cm.name, cm.type));
    legacy.regularColumns().forEach(cm -> builder.addRegularColumn(cm.name, cm.type.freeze()));
    return builder.build();
  }
}
