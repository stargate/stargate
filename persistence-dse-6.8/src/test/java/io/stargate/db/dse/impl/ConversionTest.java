package io.stargate.db.dse.impl;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.DefaultQueryOptions;
import io.stargate.db.QueryOptions;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConversionTest {

  private static ByteBuffer bytes(String v) {
    return UTF8Type.instance.decompose(v);
  }

  // The name is all we care about in this class, so we put the rest to random values.
  private static ColumnSpecification spec(String name) {
    ColumnIdentifier id = ColumnIdentifier.getInterned(name, true);
    return new ColumnSpecification("ks", "tbl", id, UTF8Type.instance);
  }

  @BeforeAll
  public static void setup() {
    // Wonderfully, the org.apache.cassandra.transport.ProtocolVersion#decode method, which ends up
    // being used as part those tests, calls the DataDescriptor singleton, so we need to initialize
    // it.
    DatabaseDescriptor.clientInitialization();
  }

  @Test
  public void testQueryOptionsConversion() {

    List<ByteBuffer> values = asList(bytes("world"), bytes("hello"));
    List<String> names = asList("v2", "v1");
    // QueryOptions deserialize the paging state so we need to have something valid. We really
    // don't care otherwise, so this is the simplest paging state bytes that deserialize and
    // is equal to itself when re-serialized.
    ByteBuffer pagingState = ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    // Test a case that uses all non-default options (we skip nowInSeconds, which is a protocol v5
    // feature and is not implemented by DSE 6.8 yet).
    QueryOptions options =
        DefaultQueryOptions.builder()
            .consistency(ConsistencyLevel.LOCAL_QUORUM)
            .skipMetadata(true)
            .values(values)
            .names(names)
            .version(ProtocolVersion.V3)
            .options(
                DefaultQueryOptions.SpecificOptions.builder()
                    .serialConsistency(ConsistencyLevel.LOCAL_SERIAL)
                    .pageSize(15)
                    .pagingState(pagingState)
                    .timestamp(123456)
                    .nowInSeconds(123)
                    .keyspace("foobar")
                    .build())
            .build();

    org.apache.cassandra.cql3.QueryOptions converted = Conversion.toInternal(options);

    // We have to prepare() to check the named parameters are re-ordered properly.
    converted = converted.prepare(asList(spec("v1"), spec("v2")));

    assertThat(converted.getConsistency())
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM);
    assertThat(converted.skipMetadata()).isTrue();
    // Again, due to the names, we expect the values to have been re-ordered
    assertThat(converted.getValues()).isEqualTo(asList(bytes("hello"), bytes("world")));
    assertThat(converted.getProtocolVersion())
        .isEqualTo(org.apache.cassandra.transport.ProtocolVersion.V3);
    // Since we don't use the default, we should we good passing null
    assertThat(converted.getSerialConsistency(null))
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(converted.getPagingOptions().pageSize().isInRows()).isTrue();
    assertThat(converted.getPagingOptions().pageSize().inRows()).isEqualTo(15);
    assertThat(converted.getPagingOptions().state()).isEqualTo(pagingState);
    assertThat(converted.getTimestamp()).isEqualTo(123456);
    assertThat(converted.getKeyspace()).isEqualTo("foobar");
  }
}
