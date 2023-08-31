package io.stargate.db.cassandra.impl;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.SimpleDataSet;

public class StargatePeersView extends StargateNodeView {
  StargatePeersView() {
    super(
        StargateSystemKeyspace.virtualFromLegacy(
            SystemKeyspace.metadata().getTableNullable(SystemKeyspace.PEERS_V2),
            StargateSystemKeyspace.PEERS_V2_TABLE_NAME));
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
                  .row(p.getPeer().getAddress(), p.getPeer().getPort())
                  // + "preferred_ip inet,"
                  .column("preferred_ip", p.getRpcAddress())
                  // + "preferred_port int,"
                  .column("preferred_port", p.getRpcPort())
                  // + "native_address inet,"
                  .column("native_address", p.getNativeAddress())
                  // + "native_port int,"
                  .column("native_port", p.getNativePort());

              completeRow(dataset, p);
            });

    return dataset;
  }
}
