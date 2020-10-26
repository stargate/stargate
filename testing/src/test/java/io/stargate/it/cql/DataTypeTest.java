package io.stargate.it.cql;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.it.cql.DataTypeTest.TypeSample.typeSample;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

public class DataTypeTest extends JavaDriverTestBase {

  @Test
  @DisplayName("Should write and read all data types")
  public void readWriteTest() {

    List<TypeSample<?>> types = generateAllTypes(keyspaceId);

    createSchema(types);

    // This should be a Junit parameterized test, but because of the way BaseOsgiIntegrationTest is
    // implemented, it would start a new Stargate container for each execution.
    // TODO refactor BaseOsgiIntegrationTest into a Junit extension
    for (TypeSample<?> sample : types) {
      try {
        should_write_and_read(sample);
      } catch (Throwable t) {
        throw new AssertionFailedError(
            String.format(
                "Error on %s (cqlType=%s, value=%s)",
                sample.columnName,
                sample.cqlType.asCql(true, true),
                session
                    .getContext()
                    .getCodecRegistry()
                    .codecFor(sample.cqlType)
                    .format(sample.value)),
            t);
      }
    }
  }

  private void createSchema(List<TypeSample<?>> types) {
    // Creating a new table for each type is too slow, use a single table with all possible types:
    CreateTable createTableQuery = createTable("test").withPartitionKey("k", DataTypes.INT);
    for (TypeSample<?> sample : types) {
      createTableQuery = createTableQuery.withColumn(sample.columnName, sample.cqlType);
      if (sample.cqlType instanceof UserDefinedType) {
        session.execute(((UserDefinedType) sample.cqlType).describe(false));
      }
    }
    session.execute(createTableQuery.asCql());
  }

  private <JavaTypeT> void should_write_and_read(TypeSample<JavaTypeT> sample) {

    String insertQuery =
        insertInto("test").value("k", literal(1)).value(sample.columnName, bindMarker()).asCql();

    SimpleStatement simpleStatement = SimpleStatement.newInstance(insertQuery, sample.value);
    session.execute(simpleStatement);
    checkValue(sample);

    session.execute(BatchStatement.newInstance(BatchType.LOGGED).add(simpleStatement));
    checkValue(sample);

    session.execute(session.prepare(insertQuery).bind(sample.value));
    checkValue(sample);
  }

  private <JavaTypeT> void checkValue(TypeSample<JavaTypeT> sample) {
    String selectQuery =
        selectFrom("test").column(sample.columnName).whereColumn("k").isEqualTo(literal(1)).asCql();
    Row row = session.execute(selectQuery).one();
    assertThat(row).isNotNull();
    assertThat(row.get(0, sample.javaType)).isEqualTo(sample.value);

    // Clean up for the following tests
    session.execute("DELETE FROM test WHERE k = 1");
  }

  private static List<TypeSample<?>> generateAllTypes(CqlIdentifier keyspaceId) {
    List<TypeSample<?>> primitiveTypes =
        ImmutableList.of(
            typeSample(DataTypes.ASCII, GenericType.STRING, "sample ascii"),
            typeSample(DataTypes.BIGINT, GenericType.LONG, Long.MAX_VALUE),
            typeSample(
                DataTypes.BLOB, GenericType.of(ByteBuffer.class), Bytes.fromHexString("0xCAFE")),
            typeSample(DataTypes.BOOLEAN, GenericType.BOOLEAN, Boolean.TRUE),
            typeSample(DataTypes.DECIMAL, GenericType.BIG_DECIMAL, new BigDecimal("12.3E+7")),
            typeSample(DataTypes.DOUBLE, GenericType.DOUBLE, Double.MAX_VALUE),
            typeSample(DataTypes.FLOAT, GenericType.FLOAT, Float.MAX_VALUE),
            typeSample(DataTypes.INET, GenericType.INET_ADDRESS, sampleInet()),
            typeSample(DataTypes.TINYINT, GenericType.BYTE, Byte.MAX_VALUE),
            typeSample(DataTypes.SMALLINT, GenericType.SHORT, Short.MAX_VALUE),
            typeSample(DataTypes.INT, GenericType.INTEGER, Integer.MAX_VALUE),
            typeSample(DataTypes.DURATION, GenericType.CQL_DURATION, CqlDuration.from("PT30H20M")),
            typeSample(DataTypes.TEXT, GenericType.STRING, "sample text"),
            typeSample(
                DataTypes.TIMESTAMP, GenericType.INSTANT, Instant.ofEpochMilli(872835240000L)),
            typeSample(DataTypes.DATE, GenericType.LOCAL_DATE, LocalDate.ofEpochDay(16071)),
            typeSample(
                DataTypes.TIME, GenericType.LOCAL_TIME, LocalTime.ofNanoOfDay(54012123450000L)),
            typeSample(
                DataTypes.TIMEUUID,
                GenericType.UUID,
                UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66")),
            typeSample(
                DataTypes.UUID,
                GenericType.UUID,
                UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")),
            typeSample(
                DataTypes.VARINT,
                GenericType.BIG_INTEGER,
                new BigInteger(Integer.MAX_VALUE + "000")));

    List<TypeSample<?>> allTypes = new ArrayList<>();
    for (TypeSample<?> type : primitiveTypes) {
      // Generate additional samples from each primitive
      allTypes.add(type);
      allTypes.add(listOf(type));
      if (type.javaType != GenericType.CQL_DURATION) {
        allTypes.add(setOf(type));
        allTypes.add(mapToIntFrom(type));
      }
      allTypes.add(mapOfIntTo(type));
      allTypes.add(tupleOfIntAnd(type));
      allTypes.add(udtOfIntAnd(type, keyspaceId));
    }
    return allTypes;
  }

  static class TypeSample<JavaTypeT> {

    static <JavaTypeT> TypeSample<JavaTypeT> typeSample(
        DataType cqlType,
        GenericType<JavaTypeT> javaType,
        JavaTypeT value,
        CqlIdentifier columnName) {
      return new TypeSample<>(cqlType, javaType, value, columnName);
    }

    static <JavaTypeT> TypeSample<JavaTypeT> typeSample(
        DataType cqlType, GenericType<JavaTypeT> javaType, JavaTypeT value) {
      return typeSample(cqlType, javaType, value, newColumnName());
    }

    static CqlIdentifier newColumnName() {
      return CqlIdentifier.fromCql("column" + COLUMN_COUNTER.getAndIncrement());
    }

    final DataType cqlType;
    final GenericType<JavaTypeT> javaType;
    final JavaTypeT value;
    final CqlIdentifier columnName;

    private TypeSample(
        DataType cqlType,
        GenericType<JavaTypeT> javaType,
        JavaTypeT value,
        CqlIdentifier columnName) {
      this.cqlType = cqlType;
      this.javaType = javaType;
      this.value = value;
      this.columnName = columnName;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", TypeSample.class.getSimpleName() + "[", "]")
          .add("columnName=" + columnName)
          .add("cqlType=" + cqlType)
          .add("javaType=" + javaType)
          .toString();
    }

    private static final AtomicInteger COLUMN_COUNTER = new AtomicInteger();
  }

  private static <JavaTypeT> TypeSample<List<JavaTypeT>> listOf(TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.listOf(type.cqlType),
        GenericType.listOf(type.javaType),
        ImmutableList.of(type.value));
  }

  private static <JavaTypeT> TypeSample<Set<JavaTypeT>> setOf(TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.setOf(type.cqlType),
        GenericType.setOf(type.javaType),
        ImmutableSet.of(type.value));
  }

  private static <JavaTypeT> TypeSample<Map<Integer, JavaTypeT>> mapOfIntTo(
      TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.mapOf(DataTypes.INT, type.cqlType),
        GenericType.mapOf(GenericType.INTEGER, type.javaType),
        ImmutableMap.of(1, type.value));
  }

  private static <JavaTypeT> TypeSample<Map<JavaTypeT, Integer>> mapToIntFrom(
      TypeSample<JavaTypeT> type) {
    return typeSample(
        DataTypes.mapOf(type.cqlType, DataTypes.INT),
        GenericType.mapOf(type.javaType, GenericType.INTEGER),
        ImmutableMap.of(type.value, 1));
  }

  private static <JavaTypeT> TypeSample<TupleValue> tupleOfIntAnd(TypeSample<JavaTypeT> type) {
    TupleType tupleType = DataTypes.tupleOf(DataTypes.INT, type.cqlType);
    return typeSample(tupleType, GenericType.TUPLE_VALUE, tupleType.newValue(1, type.value));
  }

  private static <JavaTypeT> TypeSample<UdtValue> udtOfIntAnd(
      TypeSample<JavaTypeT> type, CqlIdentifier keyspaceId) {
    CqlIdentifier columnName = TypeSample.newColumnName();
    UserDefinedType udtType =
        new UserDefinedTypeBuilder(keyspaceId, CqlIdentifier.fromInternal(columnName + "Type"))
            .withField("v1", DataTypes.INT)
            .withField("v2", type.cqlType)
            .build();
    UdtValue udtValue = udtType.newValue(1, type.value);
    return typeSample(udtType, GenericType.UDT_VALUE, udtValue, columnName);
  }

  private static InetAddress sampleInet() {
    InetAddress address;
    try {
      address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    } catch (UnknownHostException uhae) {
      throw new AssertionError("Could not get address from 127.0.0.1", uhae);
    }
    return address;
  }
}
