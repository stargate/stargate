package io.stargate.db.cassandra.impl;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.BootstrapState;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.commons.lang3.ArrayUtils;

public class StargateSystemKeyspace {
  public static final String SYSTEM_KEYSPACE_NAME = "stargate_system";
  public static final String LOCAL_TABLE_NAME = "local";
  public static final String PEERS_TABLE_NAME = "peers";
  public static final String PEERS_V2_TABLE_NAME = "peers_v2";

  public static final UUID SCHEMA_VERSION = UUID.fromString("17846767-28a1-4acd-a967-f609ff1375f1");

  public static final TableMetadata Local =
      parse(
              LOCAL_TABLE_NAME,
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
                  + "PRIMARY KEY ((key)))")
          .recordColumnDrop(
              ColumnMetadata.regularColumn(
                  SYSTEM_KEYSPACE_NAME, LOCAL_TABLE_NAME, "thrift_version", UTF8Type.instance),
              Long.MAX_VALUE) // Record deprecated
          .build();

  public static final TableMetadata Peers =
      parse(
              PEERS_TABLE_NAME,
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

  public static final TableMetadata PeersV2 =
      parse(
              PEERS_V2_TABLE_NAME,
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

  private static TableMetadata.Builder parse(String table, String description, String cql) {
    return CreateTableStatement.parse(format(cql, table), SYSTEM_KEYSPACE_NAME)
        .id(forSystemTable(SYSTEM_KEYSPACE_NAME, table))
        .gcGraceSeconds(0)
        .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
        .comment(description);
  }

  /** Copy of {@link TableId#forSystemTable(String, String)} without assertion. */
  private static TableId forSystemTable(String keyspace, String table) {
    return TableId.fromUUID(
        UUID.nameUUIDFromBytes(
            ArrayUtils.addAll(
                // Keyspace and tables names are limited to [a-zA-Z_0-9]+, so ASCII will do
                keyspace.getBytes(StandardCharsets.US_ASCII),
                table.getBytes(StandardCharsets.US_ASCII))));
  }

  public static Tables tables() {
    return Tables.of(Local, Peers, PeersV2);
  }

  public static KeyspaceMetadata metadata() {
    return KeyspaceMetadata.create(
        SYSTEM_KEYSPACE_NAME,
        KeyspaceParams.local(),
        tables(),
        Views.none(),
        Types.none(),
        UserFunctions.none());
  }

  public static void persistLocalMetadata() {
    String req =
        "INSERT INTO %s.%s ("
            + "key,"
            + "cluster_name,"
            + "release_version,"
            + "cql_version,"
            + "native_protocol_version,"
            + "data_center,"
            + "rack,"
            + "partitioner,"
            + "rpc_address,"
            + "rpc_port,"
            + "broadcast_address,"
            + "broadcast_port,"
            + "listen_address,"
            + "listen_port,"
            + "bootstrapped,"
            + "host_id,"
            + "tokens,"
            + "schema_version"
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    executeOnceInternal(
        format(req, SYSTEM_KEYSPACE_NAME, LOCAL_TABLE_NAME),
        SystemKeyspace.LOCAL,
        DatabaseDescriptor.getClusterName(),
        FBUtilities.getReleaseVersionString(),
        QueryProcessor.CQL_VERSION.toString(),
        String.valueOf(ProtocolVersion.CURRENT.asInt()),
        snitch.getLocalDatacenter(),
        snitch.getLocalRack(),
        DatabaseDescriptor.getPartitioner().getClass().getName(),
        FBUtilities.getJustBroadcastNativeAddress(),
        DatabaseDescriptor.getNativeTransportPort(),
        FBUtilities.getJustBroadcastAddress(),
        DatabaseDescriptor.getStoragePort(),
        FBUtilities.getJustLocalAddress(),
        DatabaseDescriptor.getStoragePort(),
        BootstrapState.COMPLETED.name(),
        Nodes.local().get().getHostId(),
        StargateSystemKeyspace.generateRandomTokens(
            FBUtilities.getBroadcastNativeAddressAndPort(), DatabaseDescriptor.getNumTokens()),
        SCHEMA_VERSION);
  }

  public static boolean isSystemPeers(SelectStatement statement) {
    return statement.columnFamily().equals(SystemKeyspace.LEGACY_PEERS);
  }

  public static boolean isSystemPeersV2(SelectStatement statement) {
    return statement.columnFamily().equals(SystemKeyspace.PEERS_V2);
  }

  public static boolean isSystemLocalOrPeers(CQLStatement statement) {
    if (statement instanceof SelectStatement) {
      SelectStatement selectStatement = (SelectStatement) statement;
      return selectStatement.keyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME)
          && (selectStatement.columnFamily().equals(SystemKeyspace.LOCAL)
              || isSystemPeers(selectStatement)
              || isSystemPeersV2(selectStatement));
    }
    return false;
  }

  public static synchronized void updatePeerInfo(
      InetAddressAndPort ep, String columnName, Object value) {
    if (ep.equals(FBUtilities.getBroadcastAddressAndPort())) return;

    String req = "INSERT INTO %s.%s (peer, %s) VALUES (?, ?)";
    executeInternal(
        String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME, columnName), ep.address, value);
    // This column doesn't match across the two tables
    if (columnName.equals("rpc_address")) {
      columnName = "native_address";
    }
    req = "INSERT INTO %s.%s (peer, peer_port, %s) VALUES (?, ?, ?)";
    executeInternal(
        String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME, columnName),
        ep.address,
        ep.port,
        value);
  }

  public static synchronized void updatePeerNativeAddress(
      InetAddressAndPort ep, InetAddressAndPort address) {
    if (ep.equals(FBUtilities.getBroadcastAddressAndPort())) return;

    String req = "INSERT INTO %s.%s (peer, rpc_address) VALUES (?, ?)";
    executeInternal(
        String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME), ep.address, address.address);
    req = "INSERT INTO %s.%s (peer, peer_port, native_address, native_port) VALUES (?, ?, ?, ?)";
    executeInternal(
        String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME),
        ep.address,
        ep.port,
        address.address,
        address.port);
  }

  public static synchronized void removeEndpoint(InetAddressAndPort ep) {
    String req = "DELETE FROM %s.%s WHERE peer = ?";
    executeInternal(String.format(req, SYSTEM_KEYSPACE_NAME, PEERS_TABLE_NAME), ep.address);
    req =
        String.format(
            "DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
            SYSTEM_KEYSPACE_NAME, PEERS_V2_TABLE_NAME);
    executeInternal(req, ep.address, ep.port);
    forceBlockingFlush(PEERS_TABLE_NAME, PEERS_V2_TABLE_NAME);
  }

  public static void forceBlockingFlush(String... cfnames) {
    if (!DatabaseDescriptor.isUnsafeSystem()) {
      List<ListenableFuture<CommitLogPosition>> futures = new ArrayList<>();

      for (String cfname : cfnames) {
        futures.add(
            Keyspace.open(SYSTEM_KEYSPACE_NAME)
                .getColumnFamilyStore(cfname)
                .forceFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED));
      }
      FBUtilities.waitOnFutures(futures);
    }
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
        ByteBuffer.allocate(size).put(inetAddress.addressBytes).putInt(inetAddress.port);
    bytes.rewind();
    return MurmurHash.hash2_64(bytes, bytes.position(), bytes.remaining(), 0);
  }
}
