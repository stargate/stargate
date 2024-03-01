package io.stargate.db.dse.impl;

import com.datastax.bdp.db.nodes.virtual.NodesSystemViews;
import org.apache.cassandra.db.virtual.DataSet;

public class StargatePeersView extends StargateNodeView {
  StargatePeersView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            NodesSystemViews.Peers, StargateSystemKeyspace.PEERS_TABLE_NAME));
  }

  @Override
  @SuppressWarnings("RxReturnValueIgnored")
  public DataSet data() {
    DataSet result = newDataSet();

    for (StargatePeerInfo info : StargateSystemKeyspace.instance.getPeers().values()) {
      info = info.copy();

      result.addRow(
          info.getPeer(),
          completeRow(
              result
                  .newRowBuilder()
                  // + "preferred_ip inet,"
                  .addColumn("preferred_ip", info::getRpcAddress),
              info));
    }

    return result;
  }
}
