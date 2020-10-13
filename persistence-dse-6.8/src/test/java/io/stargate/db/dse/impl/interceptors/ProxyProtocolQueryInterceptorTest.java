package io.stargate.db.dse.impl.interceptors;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.dropwizard.util.Sets;
import io.reactivex.Single;
import io.stargate.db.DefaultQueryOptions;
import io.stargate.db.QueryOptions;
import io.stargate.db.Result;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Result.Rows;
import io.stargate.db.dse.DsePersistenceActivator;
import io.stargate.db.dse.impl.ClientStateWrapper;
import io.stargate.db.dse.impl.QueryStateWrapper;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.virtual.VirtualTables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProxyProtocolQueryInterceptorTest {
  private final InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 9042);
  private final InetSocketAddress publicAddress = new InetSocketAddress("127.0.0.2", 9042);
  private final QueryStateWrapper queryState = new QueryStateWrapper(ClientStateWrapper.forExternalCalls(
      null, remoteAddress, publicAddress));
  private final QueryOptions queryOptions = DefaultQueryOptions.builder().build();

  private static File baseDir;

  private static final Map<String, AbstractType> types = ImmutableMap.of(
      "rpc_address", InetAddressType.instance
  );

  @BeforeAll
  public static void setup() throws IOException {
    baseDir = Files.createTempDirectory("stargate-dse-test").toFile();
    baseDir.deleteOnExit();
    DatabaseDescriptor.daemonInitialization(true, DsePersistenceActivator.makeConfig(baseDir));
    VirtualTables.initialize();
  }

  @SuppressWarnings({"unchecked"})
  private static <T> T columnValue(List<ByteBuffer> row, ResultMetadata metadata, String name) {
    OptionalInt index =
        IntStream.range(0, metadata.columnCount).filter(i -> metadata.columns.get(i).name().equals(name)).findFirst();
    assertThat(index).isPresent();

    AbstractType<?> type = types.get(name);
    assertThat(type).isNotNull();

    return (T) type.compose(row.get(index.getAsInt()));
  }

  @Test
  public void systemLocal() {
    ProxyProtocolQueryInterceptor interceptor = new ProxyProtocolQueryInterceptor();
    CQLStatement statement = QueryProcessor.parseStatement("SELECT * FROM system.local", queryState.getWrapped());
    interceptor.initialize();
    Single<Result> single = interceptor.interceptQuery(statement, queryState, queryOptions, null, 0);

    Rows rowsResult = (Rows)single.blockingGet();
    assertThat(rowsResult.rows).isNotEmpty();
    assertThat(rowsResult.rows.get(0).size()).isEqualTo(rowsResult.resultMetadata.columnCount);

    List<ByteBuffer> row = rowsResult.rows.get(0);
    InetAddress rpcAddress = columnValue(row, rowsResult.resultMetadata, "rpc_address");
    assertThat(rpcAddress).isEqualTo(publicAddress.getAddress());
  }

  @Test
  public void systemPeers() {
    ProxyProtocolQueryInterceptor interceptor = new ProxyProtocolQueryInterceptor(name -> {
      return Sets.of(InetAddress.getByName("127.0.0.3"), InetAddress.getByName("127.0.0.4"));
    }, "test", 1);

    CQLStatement statement = QueryProcessor.parseStatement("SELECT * FROM system.peers", queryState.getWrapped());
    interceptor.initialize();
    Single<Result> single = interceptor.interceptQuery(statement, queryState, queryOptions, null, 0);
  }
}
