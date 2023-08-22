package io.stargate.db.cassandra.impl;

import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.schema.TableMetadata;

public abstract class StargateNodeView extends AbstractVirtualTable {
  StargateNodeView(TableMetadata metadata) {
    super(metadata);
  }

  DataSet completeRow(SimpleDataSet dataset, StargateNodeInfo info) {
    System.out.println("StargateNodeView.completeRow: info.host_id=" + info.getHostId());
    // + "data_center text,"
    return dataset
        .column("data_center", info.getDataCenter())
        // + "host_id uuid,"
        .column("host_id", info.getHostId())
        // + "rack text,"
        .column("rack", info.getRack())
        // + "release_version text,"
        .column("release_version", safeToString(info.getReleaseVersion()))
        // + "schema_version uuid,"
        .column("schema_version", StargateSystemKeyspace.SCHEMA_VERSION)
        // + "tokens set<varchar>,"
        .column("tokens", info.getTokens());
  }

  static String safeToString(Object o) {
    return o != null ? o.toString() : null;
  }
}
