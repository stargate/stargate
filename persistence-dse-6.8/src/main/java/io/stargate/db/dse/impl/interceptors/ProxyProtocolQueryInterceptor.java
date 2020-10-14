package io.stargate.db.dse.impl.interceptors;

import static io.stargate.db.dse.impl.StargateSystemKeyspace.isSystemLocalOrPeers;

import com.datastax.bdp.db.nodes.virtual.LocalNodeSystemView;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;
import io.reactivex.Single;
import io.stargate.db.dse.impl.ClientStateWithPublicAddress;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A query interceptor that echos back the public IPs from the proxy protocol from `system.local`.
 * The goal is to populate `system.peers` with A-records from a provided DNS name.
 */
public class ProxyProtocolQueryInterceptor implements QueryInterceptor {
  @Override
  public void initialize() {}

  @Override
  public Single<ResultMessage> interceptQuery(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    if (!isSystemLocalOrPeers(statement)) {
      return null;
    }

    SelectStatement selectStatement = (SelectStatement) statement;

    String tableName = selectStatement.table();
    if (tableName.equals(PeersSystemView.NAME)) {
      // Returning an empty result for now, but this will return the result of the proxy's DNS A
      // records in the future
      return Single.just(
          new ResultMessage.Rows(
              new org.apache.cassandra.cql3.ResultSet(
                  ((SelectStatement) statement).getResultMetadata())));
    } else {
      assert tableName.equals(LocalNodeSystemView.NAME);

      Single<ResultMessage> resp =
          QueryProcessor.instance.processStatement(
              statement, state, options, customPayload, queryStartNanoTime);

      return resp.map(
          (result) -> {
            if (result.kind == ResultMessage.Kind.ROWS) {
              ResultMessage.Rows rows = (ResultMessage.Rows) result;
              if (!rows.result.isEmpty()) {
                ResultSet.ResultMetadata metadata = rows.result.metadata;

                ClientState clientState = state.getClientState();
                assert clientState instanceof ClientStateWithPublicAddress;
                InetSocketAddress inetAddress =
                    ((ClientStateWithPublicAddress) clientState).publicAddress();

                InetAddress publicAddress = inetAddress.getAddress();
                int publicPort = inetAddress.getPort();

                int index = 0;
                // Intercept and replace all address/port entries with the proxy protocol's public
                // address and port
                for (ColumnSpecification column : metadata.names) {
                  switch (column.name.toString()) {
                    case "rpc_address":
                    case "peer_address":
                    case "broadcast_address":
                    case "native_transport_address":
                    case "listen_address":
                      rows.result
                          .rows
                          .get(0)
                          .set(index, InetAddressType.instance.decompose(publicAddress));
                      break;
                    case "native_transport_port":
                    case "native_transport_port_ssl":
                      rows.result.rows.get(0).set(index, Int32Type.instance.decompose(publicPort));
                      break;
                    case "host_id":
                      // Return a deterministic entry for `host_id` based on the public address.
                      rows.result
                          .rows
                          .get(0)
                          .set(
                              index,
                              UUIDType.instance.decompose(
                                  UUID.nameUUIDFromBytes(publicAddress.getAddress())));
                      break;
                    case "tokens":
                      // All entries handle the entire token ring. This prevents some driver from
                      // crashing when `tokens` is null.
                      rows.result
                          .rows
                          .get(0)
                          .set(
                              index,
                              SetType.getInstance(UTF8Type.instance, false)
                                  .decompose(
                                      Collections.singleton(
                                          DatabaseDescriptor.getPartitioner()
                                              .getMinimumToken()
                                              .toString())));
                  }
                  ++index;
                }
              }
            }
            return result;
          });
    }
  }

  @Override
  public void register(IEndpointLifecycleSubscriber subscriber) {
    // TODO: Monitor A-records for DNS to generate events for stargate peers
  }
}
