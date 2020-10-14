package io.stargate.db.cassandra.impl;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
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

  @Test
  public void testAllNonDefaultQueryOptionsConversion() {
    List<ByteBuffer> values = asList(bytes("world"), bytes("hello"));
    List<String> names = asList("v2", "v1");
    // QueryOptions deserialize the paging state so we need to have something valid. We really
    // don't care otherwise, so this is the simplest paging state bytes that deserialize and
    // is equal to itself when re-serialized.
    ByteBuffer pagingState = ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    // Test a case that uses all non-default options.
    Parameters parameters =
        ImmutableParameters.builder()
            .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .skipMetadataInResult(true)
            .protocolVersion(ProtocolVersion.V3)
            .serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
            .pageSize(15)
            .pagingState(pagingState)
            .defaultTimestamp(123456)
            .nowInSeconds(123)
            .defaultKeyspace("foobar")
            .build();

    QueryOptions converted = Conversion.toInternal(values, names, parameters);

    // We have to prepare() to check the named parameters are re-ordered properly.
    converted = converted.prepare(asList(spec("v1"), spec("v2")));

    assertThat(converted.getConsistency())
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM);
    assertThat(converted.skipMetadata()).isTrue();
    // Again, due to the names, we expect the values to have been re-ordered
    assertThat(converted.getValues()).isEqualTo(asList(bytes("hello"), bytes("world")));
    assertThat(converted.getProtocolVersion())
        .isEqualTo(org.apache.cassandra.transport.ProtocolVersion.V3);
    assertThat(converted.getSerialConsistency())
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(converted.getPageSize()).isEqualTo(15);
    assertThat(
            converted.getPagingState().serialize(org.apache.cassandra.transport.ProtocolVersion.V3))
        .isEqualTo(pagingState);
    // Since we don't use the default, we should we good passing null
    assertThat(converted.getTimestamp(null)).isEqualTo(123456);
    assertThat(converted.getNowInSeconds(null)).isEqualTo(123);
    assertThat(converted.getKeyspace()).isEqualTo("foobar");
  }

  @Test
  public void testAllDefaultQueryOptionsConversion() {
    // Test a case that uses all default options.
    QueryOptions converted =
        Conversion.toInternal(Collections.emptyList(), null, Parameters.defaults());

    QueryState queryState = QueryState.forInternalCalls();

    // Using prepare to emulate real usage.
    converted = converted.prepare(Collections.emptyList());

    assertThat(converted.getConsistency()).isEqualTo(org.apache.cassandra.db.ConsistencyLevel.ONE);
    assertThat(converted.skipMetadata()).isFalse();
    // Again, due to the names, we expect the values to have been re-ordered
    assertThat(converted.getValues()).isEmpty();
    assertThat(converted.getProtocolVersion())
        .isEqualTo(Conversion.toInternal(ProtocolVersion.CURRENT));
    assertThat(converted.getSerialConsistency())
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.SERIAL);
    assertThat(converted.getPageSize()).isEqualTo(-1);
    assertThat(converted.getPagingState()).isNull();
    // The timestamp and nowInSecond are going to be basically the server time. We don't care about
    // the details, let's just make sure it's not obviously broken
    assertThat(converted.getTimestamp(queryState)).isGreaterThan(0);
    assertThat(converted.getNowInSeconds(queryState)).isGreaterThan(0);
    assertThat(converted.getKeyspace()).isNull();
  }
}
