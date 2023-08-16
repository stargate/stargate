package io.stargate.db.cassandra.impl;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;

public class StargateLegacyPeersView extends StargateNodeView {
  StargateLegacyPeersView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            SystemKeyspace.metadata().getTableNullable(SystemKeyspace.LEGACY_PEERS),
            StargateSystemKeyspace.LEGACY_PEERS_TABLE_NAME));
  }

  @Override
  @SuppressWarnings("RxReturnValueIgnored")
  public AbstractVirtualTable.DataSet data() {

    SimpleDataSet dataset = new SimpleDataSet(metadata());

    StargateSystemKeyspace.instance.getPeers().values().stream()
        .forEach(
            p -> {
              // Have to copy the current PeerInfo object as it may change while we're constructing
              // the row,
              // so null-values could sneak in and cause NPEs during serialization.
              p = p.copy();

              dataset
                  .row(p.getPeer().getAddress())
                  // + "preferred_ip inet,"
                  .column("preferred_ip", p.getRpcAddress())
                  // + "rpc_address inet,"
                  .column("rpc_address", p.getRpcAddress());
              completeRow(dataset, p);
            });

    return dataset;
  }
}
