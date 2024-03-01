package io.stargate.db.dse.impl;

import com.datastax.bdp.db.nodes.BootstrapState;
import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.NodesSystemViews;
import org.apache.cassandra.db.virtual.DataSet;

public class StargateLocalView extends StargateNodeView {
  StargateLocalView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            NodesSystemViews.Local, StargateSystemKeyspace.LOCAL_TABLE_NAME));
  }

  @Override
  @SuppressWarnings("RxReturnValueIgnored")
  public DataSet data() {
    DataSet result = newDataSet();

    StargateLocalInfo info = StargateSystemKeyspace.instance.getLocal().copy();

    DataSet.RowBuilder rowBuilder =
        result
            .newRowBuilder()
            // + "bootstrapped text,"
            .addColumn("bootstrapped", () -> safeToString(BootstrapState.COMPLETED))
            // + "broadcast_address inet,"
            .addColumn("broadcast_address", info::getBroadcastAddress)
            // + "cluster_name text,"
            .addColumn("cluster_name", info::getClusterName)
            // + "cql_version text,"
            .addColumn("cql_version", () -> safeToString(info.getCqlVersion()))
            // + "gossip_generation int,"
            .addColumn("gossip_generation", () -> null)
            // + "listen_address inet,"
            .addColumn("listen_address", info::getListenAddress)
            // + "native_protocol_version text,"
            .addColumn("native_protocol_version", info::getNativeProtocolVersion)
            // + "partitioner text,"
            .addColumn("partitioner", info::getPartitioner)
            // + "truncated_at map<uuid, blob>,"
            .addColumn("truncated_at", () -> null)
            .addColumn("last_nodesync_checkpoint_time", () -> null);

    result.addRow(LocalNodeSystemView.KEY, completeRow(rowBuilder, info));

    return result;
  }
}
