package io.stargate.db.cassandra.impl;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;

public class StargateSystemKeyspace extends VirtualKeyspace {
  public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
  public static final String LOCAL_TABLE_NAME = "local";
  public static final String LEGACY_PEERS_TABLE_NAME = "peers";
  public static final String PEERS_V2_TABLE_NAME = "peers_v2";

  public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

  public static final StargateSystemKeyspace instance = new StargateSystemKeyspace();

  private StargateSystemKeyspace() {
    super(
        SYSTEM_KEYSPACE_NAME,
        ImmutableList.of(
            new StargatePeersView(), new StargateLegacyPeersView(), new StargateLocalView()));
  }

  private final ConcurrentMap<InetAddressAndPort, StargatePeerInfo> peers =
      new ConcurrentHashMap<>();
  private final StargateLocalInfo local = new StargateLocalInfo();

  public ConcurrentMap<InetAddressAndPort, StargatePeerInfo> getPeers() {
    return peers;
  }

  public StargateLocalInfo getLocal() {
    return local;
  }

  public void persistLocalMetadata() {
    local.setClusterName(DatabaseDescriptor.getClusterName());
    local.setReleaseVersion(FBUtilities.getReleaseVersionString());
    local.setCqlVersion(QueryProcessor.CQL_VERSION);
    local.setNativeProtocolVersion(String.valueOf(ProtocolVersion.CURRENT.asInt()));
    local.setDataCenter(DatabaseDescriptor.getLocalDataCenter());
    local.setRack(DatabaseDescriptor.getEndpointSnitch().getLocalRack());
    local.setPartitioner(DatabaseDescriptor.getPartitioner().getClass().getName());
    local.setBroadcastAddress(FBUtilities.getJustBroadcastAddress());
    local.setListenAddress(FBUtilities.getJustLocalAddress());
    local.setNativeAddress(FBUtilities.getJustBroadcastNativeAddress());
    local.setNativePort(DatabaseDescriptor.getNativeTransportPort());
    local.setHostId(SystemKeyspace.getOrInitializeLocalHostId());
    local.setTokens(
        StargateSystemKeyspace.generateRandomTokens(
            FBUtilities.getBroadcastNativeAddressAndPort(), DatabaseDescriptor.getNumTokens()));
  }

  public static boolean isSystemPeers(CQLStatement statement) {
    if (statement instanceof SelectStatement) {
      SelectStatement selectStatement = (SelectStatement) statement;
      return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME)
          && selectStatement.table().equals(SystemKeyspace.LEGACY_PEERS);
    }
    return false;
  }

  public static boolean isSystemPeersV2(CQLStatement statement) {
    if (statement instanceof SelectStatement) {
      SelectStatement selectStatement = (SelectStatement) statement;
      return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME)
          && selectStatement.table().equals(SystemKeyspace.PEERS_V2);
    }
    return false;
  }

  public static boolean isSystemLocalOrPeers(CQLStatement statement) {
    if (statement instanceof SelectStatement) {
      SelectStatement selectStatement = (SelectStatement) statement;
      return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME)
          && (selectStatement.table().equals(SystemKeyspace.LEGACY_PEERS)
              || selectStatement.table().equals(SystemKeyspace.PEERS_V2)
              || selectStatement.table().equals(SystemKeyspace.LOCAL));
    }
    return false;
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

  public static Set<String> generateRandomTokens(InetAddressAndPort inetAddress, int numTokens) {
    Random random = new Random(getSeed(inetAddress));
    IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    TokenFactory tokenFactory = partitioner.getTokenFactory();
    Set<String> tokens = new HashSet<>(numTokens);
    while (tokens.size() < numTokens) {
      tokens.add(tokenFactory.toString(partitioner.getRandomToken(random)));
    }
    return tokens;
  }

  private static long getSeed(InetAddressAndPort inetAddress) {
    final int size = inetAddress.addressBytes.length + 4;
    ByteBuffer bytes =
        ByteBuffer.allocate(size).put(inetAddress.addressBytes).putInt(inetAddress.getPort());
    bytes.rewind();
    return MurmurHash.hash2_64(bytes, bytes.position(), bytes.remaining(), 0);
  }
}
