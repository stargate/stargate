package io.stargate.it.cql.protocolV4;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static io.stargate.it.cql.protocolV4.TypeSample.listOf;
import static io.stargate.it.cql.protocolV4.TypeSample.mapOfIntTo;
import static io.stargate.it.cql.protocolV4.TypeSample.mapToIntFrom;
import static io.stargate.it.cql.protocolV4.TypeSample.setOf;
import static io.stargate.it.cql.protocolV4.TypeSample.tupleOfIntAnd;
import static io.stargate.it.cql.protocolV4.TypeSample.typeSample;
import static io.stargate.it.cql.protocolV4.TypeSample.udtOfIntAnd;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(customOptions = "applyProtocolVersion")
public class DataTypeTest extends BaseIntegrationTest {

  private static List<TypeSample<?>> allTypes;

  @BeforeAll
  public static void createSchema(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {

    allTypes = generateAllTypes(keyspaceId);

    // Creating a new table for each type is too slow, use a single table with all possible types:
    CreateTable createTableQuery = createTable("test").withPartitionKey("k", DataTypes.INT);
    for (TypeSample<?> sample : allTypes) {
      createTableQuery = createTableQuery.withColumn(sample.columnName, sample.cqlType);
      if (sample.cqlType instanceof UserDefinedType) {
        session.execute(((UserDefinedType) sample.cqlType).describe(false));
      }
    }
    session.execute(createTableQuery.asCql());
  }

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @SuppressWarnings("unused") // called by JUnit 5
  public static List<TypeSample<?>> getAllTypes() {
    return allTypes;
  }

  @DisplayName("Should write and read data type")
  @ParameterizedTest
  @MethodSource("getAllTypes")
  public <JavaTypeT> void should_write_and_read(TypeSample<JavaTypeT> sample, CqlSession session) {

    String insertQuery =
        insertInto("test").value("k", literal(1)).value(sample.columnName, bindMarker()).asCql();

    SimpleStatement simpleStatement = SimpleStatement.newInstance(insertQuery, sample.value);
    session.execute(simpleStatement);
    checkValue(sample, session);

    session.execute(BatchStatement.newInstance(BatchType.LOGGED).add(simpleStatement));
    checkValue(sample, session);

    session.execute(session.prepare(insertQuery).bind(sample.value));
    checkValue(sample, session);
  }

  private <JavaTypeT> void checkValue(TypeSample<JavaTypeT> sample, CqlSession session) {
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
        Arrays.asList(
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
      if (!type.javaType.equals(GenericType.CQL_DURATION)) {
        allTypes.add(setOf(type));
        allTypes.add(mapToIntFrom(type));
      }
      allTypes.add(mapOfIntTo(type));
      allTypes.add(tupleOfIntAnd(type));
      allTypes.add(udtOfIntAnd(type, keyspaceId));
    }
    return allTypes;
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
