package io.stargate.db.cassandra.impl;

import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.nodes.virtual.NodesSystemViews;

public class StargateLegacyPeersView extends StargateNodeView {
  StargateLegacyPeersView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            NodesSystemViews.LegacyPeersMetadata, StargateSystemKeyspace.LEGACY_PEERS_TABLE_NAME));
  }

  @Override
  @SuppressWarnings("RxReturnValueIgnored")
  public DataSet data() {

    SimpleDataSet dataset = new SimpleDataSet(metadata());

    StargateSystemKeyspace.instance.getPeers().values().stream()
        .forEach(
            p -> {
              // Have to copy the current PeerInfo object as it may change while we're constructing
              // the row,
              // so null-values could sneak in and cause NPEs during serialization.
              p = p.copy();

              dataset
                  .row(p.getPeer().address)
                  // + "preferred_ip inet,"
                  .column("preferred_ip", p.getRpcAddress())
                  // + "rpc_address inet,"
                  .column("rpc_address", p.getRpcAddress());
              completeRow(dataset, p);
            });

    return dataset;
  }
}
