package io.stargate.db.dse.impl;

import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.DataSet;
import org.apache.cassandra.schema.TableMetadata;

public abstract class StargateNodeView extends AbstractVirtualTable {
  StargateNodeView(TableMetadata metadata) {
    super(metadata);
  }

  DataSet.RowBuilder completeRow(DataSet.RowBuilder rowBuilder, StargateNodeInfo info) {
    return rowBuilder
        // + "rpc_address inet,"
        .addColumn("rpc_address", info::getRpcAddress)
        // + "native_transport_address inet,"
        .addColumn("native_transport_address", info::getNativeAddress)
        // + "data_center text,"
        .addColumn("data_center", info::getDataCenter)

        // + "host_id uuid,"
        .addColumn("host_id", info::getHostId)
        // + "rack text,"
        .addColumn("rack", info::getRack)
        // + "release_version text,"
        .addColumn("release_version", () -> safeToString(info.getReleaseVersion()))
        // + "schema_version uuid,"
        .addColumn("schema_version", () -> StargateSystemKeyspace.SCHEMA_VERSION)
        // + "tokens set<varchar>,"
        .addColumn("tokens", info::getTokens)
        // + "native_transport_port int,"
        .addColumn("native_transport_port", info::getNativePort)
        // + "native_transport_port_ssl int,"
        .addColumn("native_transport_port_ssl", info::getNativePortSsl)
        // + "storage_port int,"
        .addColumn("storage_port", info::getStoragePort)
        // + "storage_port_ssl int,"
        .addColumn("storage_port_ssl", info::getStoragePortSsl)
        // + "jmx_port int,"
        .addColumn("jmx_port", info::getJmxPort)
        // + "dse_version text,"
        // This is set to null, otherwise the java-driver, and maybe other drivers, will attempt to
        // connect using the DSE_V1 and DSE_V2 protocols.
        .addColumn("dse_version", info::getDseVersion)
        // + "graph boolean,"
        .addColumn("graph", () -> false)
        // + "server_id text,"
        .addColumn("server_id", null)
        // + "workload text,"
        .addColumn("workload", () -> null)
        // + "workloads frozen<set<text>>,"
        .addColumn("workloads", () -> null);
  }

  static String safeToString(Object o) {
    return o != null ? o.toString() : null;
  }
}
