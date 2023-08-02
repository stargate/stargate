package io.stargate.db.cassandra.impl;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.RequestFailureReason;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.junit.jupiter.api.Nested;
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

    final QueryState queryState = QueryState.forInternalCalls();

    // We have to prepare() to check the named parameters are re-ordered properly.
    converted = converted.prepare(asList(spec("v1"), spec("v2")));

    assertThat(converted.getConsistency())
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM);
    assertThat(converted.skipMetadata()).isTrue();
    // Again, due to the names, we expect the values to have been re-ordered
    assertThat(converted.getValues()).isEqualTo(asList(bytes("hello"), bytes("world")));
    assertThat(converted.getProtocolVersion())
        .isEqualTo(org.apache.cassandra.transport.ProtocolVersion.V3);
    assertThat(converted.getSerialConsistency(queryState))
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(converted.getPageSize()).isEqualTo(new PageSize(15, PageSize.PageUnit.ROWS));
    assertThat(
            converted.getPagingState().serialize(org.apache.cassandra.transport.ProtocolVersion.V3))
        .isEqualTo(pagingState);
    // Since we don't use the default, we should we good passing null
    assertThat(converted.getTimestamp(queryState)).isEqualTo(123456);
    assertThat(converted.getNowInSeconds(queryState)).isEqualTo(123);
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
    assertThat(converted.getSerialConsistency(queryState))
        .isEqualTo(org.apache.cassandra.db.ConsistencyLevel.SERIAL);
    assertThat(converted.getPageSize().getSize()).isEqualTo(PageSize.NO_LIMIT);
    assertThat(converted.getPagingState()).isNull();
    // The timestamp and nowInSecond are going to be basically the server time. We don't care about
    // the details, let's just make sure it's not obviously broken
    assertThat(converted.getTimestamp(queryState)).isGreaterThan(0);
    assertThat(converted.getNowInSeconds(queryState)).isGreaterThan(0);
    assertThat(converted.getKeyspace()).isNull();
  }

  @Nested
  class RequestFailureReasons {
    private RequestFailureReason convert(
        org.apache.cassandra.exceptions.RequestFailureReason internal) {
      return Conversion.toExternal(
              Collections.singletonMap(InetAddressAndPort.getLoopbackAddress(), internal))
          .values()
          .iterator()
          .next();
    }

    @Test
    void unknown() {
      assertThat(convert(org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN))
          .isEqualTo(RequestFailureReason.UNKNOWN);
    }

    @Test
    void readTooManyTombstones() {
      assertThat(
              convert(
                  org.apache.cassandra.exceptions.RequestFailureReason.READ_TOO_MANY_TOMBSTONES))
          .isEqualTo(RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
    }

    @Test
    void timeout() {
      assertThat(convert(org.apache.cassandra.exceptions.RequestFailureReason.TIMEOUT))
          .isEqualTo(RequestFailureReason.TIMEOUT);
    }

    @Test
    void incompatibleSchema() {
      assertThat(convert(org.apache.cassandra.exceptions.RequestFailureReason.INCOMPATIBLE_SCHEMA))
          .isEqualTo(RequestFailureReason.INCOMPATIBLE_SCHEMA);
    }

    @Test
    void allKnownCodes() {
      for (org.apache.cassandra.exceptions.RequestFailureReason r :
          org.apache.cassandra.exceptions.RequestFailureReason.values()) {
        if (r == org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN) {
          continue;
        }

        assertThat(convert(r))
            .withFailMessage(() -> "" + r + " should not convert to UNKNOWN")
            .isNotEqualTo(RequestFailureReason.UNKNOWN);
      }
    }
  }

  @Nested
  class GetTypeFromInternal {

    @Test
    public void asciiType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(AsciiType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Ascii);
    }

    @Test
    public void longType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(LongType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Bigint);
    }

    @Test
    public void bytesType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(BytesType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Blob);
    }

    @Test
    public void byteType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(ByteType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Tinyint);
    }

    @Test
    public void booleanType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(BooleanType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Boolean);
    }

    @Test
    public void compositeType() {
      Column.ColumnType result =
          Conversion.getTypeFromInternal(CompositeType.getInstance(ByteType.instance));

      assertThat(result.rawType()).isEqualTo(Column.Type.Composite);
    }

    @Test
    public void counterColumnType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(CounterColumnType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Counter);
    }

    @Test
    public void simpleDateType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(SimpleDateType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Date);
    }

    @Test
    public void decimalType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(DecimalType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Decimal);
    }

    @Test
    public void doubleType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(DoubleType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Double);
    }

    @Test
    public void durationType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(DurationType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Duration);
    }

    @Test
    public void dynamicCompositeType() {
      Column.ColumnType result =
          Conversion.getTypeFromInternal(DynamicCompositeType.getInstance(Collections.emptyMap()));

      assertThat(result.rawType()).isEqualTo(Column.Type.DynamicComposite);
    }

    @Test
    public void floatType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(FloatType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Float);
    }

    @Test
    public void inetAddressType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(InetAddressType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Inet);
    }

    @Test
    public void int32Type() {
      Column.ColumnType result = Conversion.getTypeFromInternal(Int32Type.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Int);
    }

    @Test
    public void integerType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(IntegerType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Varint);
    }

    @Test
    public void shortType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(ShortType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Smallint);
    }

    @Test
    public void uuidType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(UUIDType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Uuid);
    }

    @Test
    public void utf8Type() {
      Column.ColumnType result = Conversion.getTypeFromInternal(UTF8Type.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Text);
    }

    @Test
    public void timeType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(TimeType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Time);
    }

    @Test
    public void timestampType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(TimestampType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Timestamp);
    }

    @Test
    public void timeUUIDType() {
      Column.ColumnType result = Conversion.getTypeFromInternal(TimeUUIDType.instance);

      assertThat(result.rawType()).isEqualTo(Column.Type.Timeuuid);
    }
  }
}
