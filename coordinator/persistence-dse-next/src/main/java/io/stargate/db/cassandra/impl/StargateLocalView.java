package io.stargate.db.cassandra.impl;

import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.nodes.BootstrapState;
import org.apache.cassandra.nodes.virtual.LocalNodeSystemView;
import org.apache.cassandra.nodes.virtual.NodesSystemViews;

public class StargateLocalView extends StargateNodeView {
  StargateLocalView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            NodesSystemViews.LocalMetadata, StargateSystemKeyspace.LOCAL_TABLE_NAME));
  }

  @Override
  @SuppressWarnings("RxReturnValueIgnored")
  public DataSet data() {
    SimpleDataSet dataset = new SimpleDataSet(metadata());

    StargateLocalInfo info = StargateSystemKeyspace.instance.getLocal().copy();

    dataset =
        dataset
            .row(LocalNodeSystemView.KEY)
            // + "bootstrapped text,"
            .column("bootstrapped", safeToString(BootstrapState.COMPLETED.name()))
            // + "broadcast_address inet,"
            .column("broadcast_address", info.getBroadcastAddress())
            // + "broadcast_port int,"
            .column("broadcast_port", info.getRpcPort())
            // + "cluster_name text,"
            .column("cluster_name", info.getClusterName())
            // + "cql_version text,"
            .column("cql_version", safeToString(info.getCqlVersion()))
            // + "data_center text,"
            .column("data_center", safeToString(info.getDataCenter()))
            // + "gossip_generation int,"
            .column("gossip_generation", null)
            // + "listen_address inet,"
            .column("listen_address", info.getListenAddress())
            // + "listen_address int,"
            .column("listen_port", info.getRpcPort())
            // + "rpc_address inet,"
            .column("rpc_address", info.getRpcAddress())
            // + "rpc_port int,"
            .column("rpc_port", info.getRpcPort())
            // + "native_protocol_version text,"
            .column("native_protocol_version", info.getNativeProtocolVersion())
            // + "partitioner text,"
            .column("partitioner", info.getPartitioner())
            // + "truncated_at map<uuid, blob>,"
            .column("truncated_at", null);

    return completeRow(dataset, info);
  }
}
