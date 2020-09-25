/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.schema;

import static io.stargate.db.schema.Column.Kind.PartitionKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.stargate.db.schema.Column.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class ColumnTest {
  @Test
  public void testFromString() throws UnknownHostException {
    assertThat(Type.Ascii.fromString("2")).isEqualTo("2");
    assertThat(Type.Bigint.fromString("2")).isEqualTo(2L);
    assertThat(Type.Blob.fromString("0x22")).isEqualTo(ByteBuffer.wrap(new byte[] {34}));
    assertThat(Type.Boolean.fromString("true")).isEqualTo(true);
    assertThat(Type.Counter.fromString("2")).isEqualTo(2L);
    assertThat(Type.Date.fromString("2001-01-01")).isEqualTo(LocalDate.of(2001, 1, 1));
    assertThat(Type.Decimal.fromString("2")).isEqualTo(new BigDecimal(2));
    assertThat(Type.Double.fromString("2.0")).isEqualTo(2d);
    assertThat(Type.Duration.fromString(CqlDuration.newInstance(2, 1, 3).toString()))
        .isEqualTo(CqlDuration.newInstance(2, 1, 3));
    assertThat(Type.Float.fromString("2")).isEqualTo(2f);
    assertThat(Type.Inet.fromString("127.0.0.1"))
        .isEqualTo(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}));
    assertThat(Type.Int.fromString("2")).isEqualTo(2);
    assertThat(Type.Smallint.fromString("2")).isEqualTo((short) 2);
    assertThat(Type.Text.fromString("2")).isEqualTo("2");
    assertThat(Type.Time.fromString("2")).isEqualTo(LocalTime.ofNanoOfDay(2));

    Instant now = Instant.now();
    assertThat(Type.Timestamp.fromString(now.toString())).isEqualTo(now);

    UUID uuid = UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f");
    assertThat(Type.Timeuuid.fromString(uuid.toString())).isEqualTo(uuid);
    assertThat(Type.Tinyint.fromString("2")).isEqualTo((byte) 2);

    assertThat(Type.Uuid.fromString(uuid.toString())).isEqualTo(uuid);
    assertThat(Type.Varchar.fromString("2")).isEqualTo("2");
    assertThat(Type.Varint.fromString("2")).isEqualTo(BigInteger.valueOf(2L));
  }

  @Test
  public void tupleFromString() {
    Column.ColumnType tupleType = Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double));
    TupleValue tuple = tupleType.create("Test", tupleType.parameters().get(1).create(2, 3.0));
    assertThat(tupleType.fromString("('Test',(2,3.0))")).isEqualTo(tuple);
  }

  @Test
  public void udtFromString() {
    Column.ColumnType tupleType = Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double));
    Keyspace ks =
        Schema.build()
            .keyspace("test")
            .type("udt1")
            .column("a", Type.Int)
            .column("mylist", Type.List.of(Type.Double))
            .column("myset", Type.Set.of(Type.Double))
            .column("mymap", Type.Map.of(Type.Text, Type.Int))
            .column("mytuple", tupleType)
            .build()
            .keyspace("test");

    java.util.List<Double> list = Arrays.asList(3.0, 4.5);
    Map<String, Integer> map = ImmutableMap.of("Alice", 3, "Bob", 4);
    Set<Double> set = ImmutableSet.of(3.4, 5.3);
    TupleValue tuple = tupleType.create("Test", tupleType.parameters().get(1).create(2, 3.0));
    assertThat(
            ks.userDefinedType("udt1")
                .fromString(
                    "{a:23,mylist:[3.0,4.5],myset:{3.4,5.3},mymap:{'Alice':3,'Bob':4},mytuple:('Test',(2,3.0))}"))
        .isEqualTo(
            ks.userDefinedType("udt1")
                .create()
                .setInt("a", 23)
                .setList("mylist", list, Double.class)
                .setMap("mymap", map, String.class, Integer.class)
                .setSet("myset", set, Double.class)
                .setTupleValue("mytuple", tuple));
  }

  @Test
  public void toStringNullValue() {
    String msg = "Parameter value cannot be null";
    assertThatThrownBy(() -> Type.Ascii.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Bigint.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Blob.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Boolean.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Counter.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Date.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Decimal.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Double.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Duration.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Float.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Int.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Smallint.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Text.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Time.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Timestamp.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Timeuuid.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Tinyint.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Uuid.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Varchar.toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Varint.toString(null)).hasMessage(msg);

    assertThatThrownBy(() -> Type.List.of(Type.Int).toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Set.of(Type.Int).toString(null)).hasMessage(msg);
    assertThatThrownBy(() -> Type.Map.of(Type.Int, Type.Int).toString(null)).hasMessage(msg);
    assertThatThrownBy(
            () -> Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double)).toString(null))
        .hasMessage(msg);

    Keyspace ks =
        Schema.build().keyspace("test").type("udt1").column("a", Type.Int).build().keyspace("test");

    assertThatThrownBy(() -> ks.userDefinedType("udt1").toString(null)).hasMessage(msg);
  }

  private String codecCannotProcessMsg(TypeCodec<?> codec, Class<?> clazz) {
    return String.format(
        "Codec '%s' cannot process value of type '%s'",
        codec.getClass().getSimpleName(), clazz.getName());
  }

  @Test
  public void testToStringWithInvalidTypes() {
    assertThatThrownBy(() -> Type.Ascii.toString(2L))
        .hasMessage(codecCannotProcessMsg(Type.Ascii.codec(), Long.class));

    assertThatThrownBy(() -> Type.Bigint.toString("2L"))
        .hasMessage(codecCannotProcessMsg(Type.Bigint.codec(), String.class));

    assertThatThrownBy(() -> Type.Blob.toString(ByteBuffer.wrap(new byte[] {34}).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Blob.codec(), String.class));

    assertThatThrownBy(() -> Type.Boolean.toString("1L"))
        .hasMessage(codecCannotProcessMsg(Type.Boolean.codec(), String.class));

    assertThatThrownBy(() -> Type.Counter.toString("2L"))
        .hasMessage(codecCannotProcessMsg(Type.Counter.codec(), String.class));

    assertThatThrownBy(() -> Type.Date.toString(LocalDate.of(2001, 1, 1).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Date.codec(), String.class));

    assertThatThrownBy(() -> Type.Decimal.toString(new BigDecimal(2).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Decimal.codec(), String.class));

    assertThatThrownBy(() -> Type.Double.toString("2d"))
        .hasMessage(codecCannotProcessMsg(Type.Double.codec(), String.class));

    assertThatThrownBy(() -> Type.Duration.toString(CqlDuration.newInstance(2, 1, 3).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Duration.codec(), String.class));

    assertThatThrownBy(() -> Type.Float.toString("2"))
        .hasMessage(codecCannotProcessMsg(Type.Float.codec(), String.class));

    assertThatThrownBy(
            () ->
                Type.Inet.toString(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Inet.codec(), String.class));

    assertThatThrownBy(() -> Type.Int.toString("2"))
        .hasMessage(codecCannotProcessMsg(Type.Int.codec(), String.class));

    assertThatThrownBy(() -> Type.Smallint.toString("2"))
        .hasMessage(codecCannotProcessMsg(Type.Smallint.codec(), String.class));

    assertThatThrownBy(() -> Type.Text.toString(2))
        .hasMessage(codecCannotProcessMsg(Type.Text.codec(), Integer.class));

    assertThatThrownBy(() -> Type.Time.toString(LocalTime.ofSecondOfDay(2).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Time.codec(), String.class));

    assertThatThrownBy(() -> Type.Timestamp.toString(Instant.ofEpochSecond(100).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Timestamp.codec(), String.class));

    assertThatThrownBy(() -> Type.Timeuuid.toString(UUID.randomUUID().toString()))
        .hasMessage(codecCannotProcessMsg(Type.Timeuuid.codec(), String.class));

    assertThatThrownBy(() -> Type.Tinyint.toString("2"))
        .hasMessage(codecCannotProcessMsg(Type.Tinyint.codec(), String.class));

    assertThatThrownBy(() -> Type.Uuid.toString(UUID.randomUUID().toString()))
        .hasMessage(codecCannotProcessMsg(Type.Uuid.codec(), String.class));

    assertThatThrownBy(() -> Type.Varchar.toString(2L))
        .hasMessage(codecCannotProcessMsg(Type.Varchar.codec(), Long.class));

    assertThatThrownBy(() -> Type.Varint.toString(BigInteger.valueOf(2L).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Varint.codec(), String.class));
  }

  @Test
  public void testToString() throws UnknownHostException {
    assertThat(Type.Ascii.toString("2")).isEqualTo("2");
    assertThat(Type.Bigint.toString(2L)).isEqualTo("2");
    assertThat(Type.Blob.toString(ByteBuffer.wrap(new byte[] {34}))).isEqualTo("0x22");
    assertThat(Type.Boolean.toString(true)).isEqualTo("true");
    assertThat(Type.Counter.toString(2L)).isEqualTo("2");
    assertThat(Type.Date.toString(LocalDate.of(2001, 1, 1))).isEqualTo("2001-01-01");
    assertThat(Type.Decimal.toString(new BigDecimal(2))).isEqualTo("2");
    assertThat(Type.Double.toString(2d)).isEqualTo("2.0");
    assertThat(Type.Duration.toString(CqlDuration.newInstance(2, 1, 3)))
        .isEqualTo(CqlDuration.newInstance(2, 1, 3).toString());
    assertThat(Type.Float.toString(2f)).isEqualTo("2.0");
    assertThat(Type.Inet.toString(InetAddress.getByAddress(new byte[] {127, 0, 0, 1})))
        .isEqualTo("127.0.0.1");
    assertThat(Type.Int.toString(2)).isEqualTo("2");
    assertThat(Type.Smallint.toString((short) 2)).isEqualTo("2");
    assertThat(Type.Text.toString("2")).isEqualTo("2");
    assertThat(Type.Time.toString(LocalTime.ofSecondOfDay(2))).isEqualTo("00:00:02.000000000");

    UUID uuid = UUID.fromString("50554d6e-29bb-11e5-b345-feff819cdc9f");
    assertThat(Type.Timeuuid.toString(uuid)).isEqualTo(uuid.toString());
    assertThat(Type.Tinyint.toString((byte) 2)).isEqualTo("2");

    assertThat(Type.Uuid.toString(uuid)).isEqualTo(uuid.toString());
    assertThat(Type.Varchar.toString("2")).isEqualTo("2");
    assertThat(Type.Varint.toString(BigInteger.valueOf(2L))).isEqualTo("2");
  }

  @Test
  public void testToStringWithCollections() {
    java.util.List<Integer> list = Arrays.asList(1, 2, 3);
    Set<Integer> set = new HashSet<>(list);
    assertThat(Type.List.of(Type.Int).toString(list)).isEqualTo("[1,2,3]");
    assertThat(Type.Set.of(Type.Int).toString(set)).isEqualTo("{1,2,3}");
    assertThat(Type.Map.of(Type.Int, Type.Int).toString(ImmutableMap.of(1, 1, 2, 2)))
        .isEqualTo("{1:1,2:2}");
  }

  @Test
  public void testToStringWithInvalidTypesOnCollections() {
    assertThatThrownBy(() -> Type.List.of(Type.Int).toString(Arrays.asList(1, 2, 3).toString()))
        .hasMessage(codecCannotProcessMsg(Type.List.of(Type.Int).codec(), String.class));

    assertThatThrownBy(
            () -> Type.Set.of(Type.Int).toString(new HashSet<>(Arrays.asList(1, 2, 3)).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Set.of(Type.Int).codec(), String.class));

    assertThatThrownBy(
            () -> Type.Map.of(Type.Int, Type.Int).toString(ImmutableMap.of(1, 1).toString()))
        .hasMessage(codecCannotProcessMsg(Type.Map.of(Type.Int, Type.Int).codec(), String.class));
  }

  @Test
  public void udtIncomplete() {
    Keyspace ks =
        Schema.build().keyspace("test").type("udt1").column("a", Type.Int).build().keyspace("test");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> ks.userDefinedType("udt1").create("a", "b"));
    assertThat(ex).hasMessage("Expected 1 parameter(s) when initializing 'udt1' but got 2");
  }

  @Test
  public void udtValidation() {
    Keyspace ks =
        Schema.build().keyspace("test").type("udt1").column("a", Type.Int).build().keyspace("test");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ks.userDefinedType("udt1").create("a"));
    assertThat(ex)
        .hasMessage(
            "Wrong value type provided for user defined type 'udt1'. Provided type 'String' is not compatible with expected CQL type 'int' at location 'a'.");
  }

  @Test
  public void tupleToString() {
    Column.ColumnType tupleType = Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double));
    TupleValue tuple = tupleType.create("Test", tupleType.parameters().get(1).create(2, 3.0));
    assertThat(tupleType.toString(tuple)).isEqualTo("('Test',(2,3.0))");
  }

  @Test
  public void tupleToStringWithInvalidType() {
    Column.ColumnType tupleType = Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double));
    TupleValue tuple = tupleType.create("Test", tupleType.parameters().get(1).create(2, 3.0));
    assertThatThrownBy(() -> tupleType.toString(tuple.toString()))
        .hasMessage(codecCannotProcessMsg(tupleType.codec(), String.class));
  }

  @Test
  public void udtToStringWithInvalidType() {
    Keyspace ks =
        Schema.build().keyspace("test").type("udt1").column("a", Type.Int).build().keyspace("test");

    UserDefinedType udt = ks.userDefinedType("udt1");
    assertThatThrownBy(() -> udt.toString(udt.create(1).toString()))
        .hasMessage(codecCannotProcessMsg(udt.codec(), String.class));
  }

  @Test
  public void udtToString() {
    // UDT
    Column.ColumnType tupleType = Type.Tuple.of(Type.Varchar, Type.Tuple.of(Type.Int, Type.Double));
    Keyspace ks =
        Schema.build()
            .keyspace("test")
            .type("udt1")
            .column("a", Type.Int)
            .column("mylist", Type.List.of(Type.Double))
            .column("myset", Type.Set.of(Type.Double))
            .column("mymap", Type.Map.of(Type.Text, Type.Int))
            .column("mytuple", tupleType)
            .build()
            .keyspace("test");

    java.util.List<java.lang.Double> list = Arrays.asList(3.0, 4.5);
    Map<String, Integer> map = ImmutableMap.of("Alice", 3, "Bob", 4);
    Set<Double> set = ImmutableSet.of(3.4, 5.3);
    TupleValue tuple = tupleType.create("Test", tupleType.parameters().get(1).create(2, 3.0));
    assertThat(
            ks.userDefinedType("udt1")
                .toString(
                    ks.userDefinedType("udt1")
                        .create()
                        .setInt("a", 23)
                        .setList("mylist", list, Double.class)
                        .setMap("mymap", map, String.class, Integer.class)
                        .setSet("myset", set, Double.class)
                        .setTupleValue("mytuple", tuple)))
        .isEqualTo(
            "{a:23,mylist:[3.0,4.5],myset:{3.4,5.3},mymap:{'Alice':3,'Bob':4},mytuple:('Test',(2,3.0))}");
  }

  @Test
  public void tupleValidation() {
    Column.ColumnType type =
        Type.Tuple.of(Type.Text, Type.List.of(Type.Tuple.of(Type.Text, Type.Int)).frozen());
    TupleValue invalid = Type.Tuple.of(Type.Int, Type.Text).create(1, "Bif");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> type.create("Zeb", Arrays.asList(invalid)));
    assertThat(e)
        .hasMessage(
            "Wrong value type provided for tuple 'frozen<tuple<text, frozen<list<frozen<tuple<text, int>>>>>>'. Provided type 'Integer' is not compatible with expected CQL type 'text' at location 'frozen<list<frozen<tuple<text, int>>>>.frozen<tuple<text, int>>[0]'.");
  }

  @Test
  public void wideningBigInteger() throws Column.ValidationException {
    // Varint is the BigInteger java type
    // widening to BigInteger works for all cases
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Varint).build().type();
    BigInteger expected = BigInteger.valueOf(Long.MAX_VALUE);
    assertThat(type.validate(expected, "x")).isEqualTo(expected);
    assertThat(type.validate(Long.MAX_VALUE, "x")).isEqualTo(expected);
    assertThat(type.validate(Integer.MAX_VALUE, "x"))
        .isEqualTo(BigInteger.valueOf(Integer.MAX_VALUE));
    assertThat(type.validate(Short.MAX_VALUE, "x")).isEqualTo(BigInteger.valueOf(Short.MAX_VALUE));
    assertThat(type.validate(Byte.MAX_VALUE, "x")).isEqualTo(BigInteger.valueOf(Byte.MAX_VALUE));
  }

  @Test
  public void wideningAndNarrowingLong() throws Column.ValidationException {
    // BigInt is the Long java type
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Bigint).build().type();
    assertThat(type.validate(Long.MAX_VALUE, "x")).isEqualTo(Long.MAX_VALUE);
    assertThat(type.validate(Integer.MAX_VALUE, "x")).isEqualTo((long) Integer.MAX_VALUE);
    assertThat(type.validate(Short.MAX_VALUE, "x")).isEqualTo((long) Short.MAX_VALUE);
    assertThat(type.validate(Byte.MAX_VALUE, "x")).isEqualTo((long) Byte.MAX_VALUE);

    // narrowing Long.MAX_VALUE to a Long should work
    assertThat(type.validate(BigInteger.valueOf(Long.MAX_VALUE), "x")).isEqualTo(Long.MAX_VALUE);

    // narrowing Long.MAX_VALUE + 1 to a Long should not work
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'bigint' but got 'BigInteger out of long range' at location 'x'");
  }

  @Test
  public void wideningAndNarrowingInt() throws Column.ValidationException {
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Int).build().type();
    assertThat(type.validate(Integer.MAX_VALUE, "x")).isEqualTo(Integer.MAX_VALUE);
    assertThat(type.validate(Short.MAX_VALUE, "x")).isEqualTo((int) Short.MAX_VALUE);
    assertThat(type.validate(Byte.MAX_VALUE, "x")).isEqualTo((int) Byte.MAX_VALUE);

    // narrowing in the range of Integer should work
    assertThat(type.validate(BigInteger.valueOf(Integer.MAX_VALUE), "x"))
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(type.validate((long) Integer.MAX_VALUE, "x")).isEqualTo(Integer.MAX_VALUE);

    // narrowing BigInteger outside of Integer range should fail
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigInteger.valueOf(Long.MAX_VALUE), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'int' but got 'BigInteger out of int range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigInteger.valueOf((long) Integer.MAX_VALUE + 1L), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'int' but got 'BigInteger out of int range' at location 'x'");

    // narrowing Integer.MAX_VALUE + 1 should fail
    e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate((long) Integer.MAX_VALUE + 1L, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'int' but got 'long out of int range' at location 'x'");
  }

  @Test
  public void wideningAndNarrowingShort() throws Column.ValidationException {
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Smallint).build().type();
    assertThat(type.validate(Short.MAX_VALUE, "x")).isEqualTo(Short.MAX_VALUE);
    assertThat(type.validate(Byte.MAX_VALUE, "x")).isEqualTo((short) Byte.MAX_VALUE);

    // narrowing in range should work
    assertThat(type.validate(BigInteger.valueOf((long) Short.MAX_VALUE), "x"))
        .isEqualTo(Short.MAX_VALUE);
    assertThat(type.validate((long) Short.MAX_VALUE, "x")).isEqualTo(Short.MAX_VALUE);
    assertThat(type.validate((int) Short.MAX_VALUE, "x")).isEqualTo(Short.MAX_VALUE);

    // narrowing outside of Short range should fail
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigInteger.valueOf((long) Short.MAX_VALUE + 1), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'smallint' but got 'BigInteger out of short range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class, () -> type.validate((long) Short.MAX_VALUE + 1, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'smallint' but got 'long out of short range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class, () -> type.validate((int) Short.MAX_VALUE + 1, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'smallint' but got 'int out of short range' at location 'x'");
  }

  @Test
  public void wideningAndNarrowingByte() throws Column.ValidationException {
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Tinyint).build().type();
    assertThat(type.validate(Byte.MAX_VALUE, "x")).isEqualTo(Byte.MAX_VALUE);

    // narrowing in range should work
    assertThat(type.validate(BigInteger.valueOf((long) Byte.MAX_VALUE), "x"))
        .isEqualTo(Byte.MAX_VALUE);
    assertThat(type.validate((long) Byte.MAX_VALUE, "x")).isEqualTo(Byte.MAX_VALUE);
    assertThat(type.validate((int) Byte.MAX_VALUE, "x")).isEqualTo(Byte.MAX_VALUE);
    assertThat(type.validate((short) Byte.MAX_VALUE, "x")).isEqualTo(Byte.MAX_VALUE);

    // narrowing outside of Byte range should fail
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigInteger.valueOf((long) Byte.MAX_VALUE + 1), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'tinyint' but got 'BigInteger out of byte range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class, () -> type.validate((long) Byte.MAX_VALUE + 1, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'tinyint' but got 'long out of byte range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class, () -> type.validate((int) Byte.MAX_VALUE + 1, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'tinyint' but got 'int out of byte range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate((short) (Byte.MAX_VALUE + 1), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'tinyint' but got 'short out of byte range' at location 'x'");
  }

  @Test
  public void wideningBigDecimal() throws Column.ValidationException {
    // widening to BigDecimal works for all cases
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Decimal).build().type();
    BigDecimal expected = BigDecimal.valueOf(java.lang.Double.MAX_VALUE);
    assertThat(type.validate(expected, "x")).isEqualTo(expected);
    assertThat(type.validate(java.lang.Double.MAX_VALUE, "x")).isEqualTo(expected);
    assertThat(type.validate(Float.MAX_VALUE, "x")).isEqualTo(BigDecimal.valueOf(Float.MAX_VALUE));
  }

  @Test
  public void wideningAndNarrowingDouble() throws Column.ValidationException {
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Double).build().type();
    assertThat(type.validate(java.lang.Double.MAX_VALUE, "x"))
        .isEqualTo(java.lang.Double.MAX_VALUE);
    assertThat(type.validate(Float.MAX_VALUE, "x")).isEqualTo((double) Float.MAX_VALUE);

    // narrowing in range should work
    assertThat(type.validate(BigDecimal.valueOf(java.lang.Double.MAX_VALUE), "x"))
        .isEqualTo(java.lang.Double.MAX_VALUE);

    // narrowing out of range should fail but it doesn't
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () ->
                type.validate(
                    BigDecimal.valueOf(java.lang.Double.MAX_VALUE).add(BigDecimal.ONE), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'double' but got 'BigDecimal out of double range' at location 'x'");
  }

  @Test
  public void wideningAndNarrowingFloat() throws Column.ValidationException {
    Column.ColumnType type = ImmutableColumn.builder().name("x").type(Type.Float).build().type();
    assertThat(type.validate(Float.MAX_VALUE, "x")).isEqualTo(Float.MAX_VALUE);
    // narrowing in range should work
    assertThat(type.validate(BigDecimal.valueOf(Float.MAX_VALUE), "x")).isEqualTo(Float.MAX_VALUE);
    assertThat(type.validate((double) Float.MAX_VALUE, "x")).isEqualTo(Float.MAX_VALUE);

    // narrowing out of range should fail
    Column.ValidationException e =
        assertThrows(
            Column.ValidationException.class,
            () -> type.validate(BigDecimal.valueOf(Float.MAX_VALUE).add(BigDecimal.ONE), "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'float' but got 'BigDecimal out of float range' at location 'x'");

    e =
        assertThrows(
            Column.ValidationException.class, () -> type.validate(java.lang.Double.MAX_VALUE, "x"));
    assertThat(e.getMessage())
        .isEqualTo("Wanted 'float' but got 'double out of float range' at location 'x'");
  }

  @Test
  public void wideningAndNarrowingForTuple() {
    Schema schema =
        Schema.build()
            .keyspace("test")
            .table("complexTypes")
            .column(
                "tuple",
                Type.Tuple.of(Type.Timestamp, Type.Tuple.of(Type.Smallint, Type.Float)),
                PartitionKey)
            .build();
    Table complexTypes = schema.keyspace("test").table("complexTypes");

    TupleValue val =
        complexTypes
            .column("tuple")
            .type()
            .create(
                Instant.now(),
                Type.Tuple.of(Type.Smallint, Type.Float)
                    .create(BigInteger.valueOf(Short.MAX_VALUE), Float.MAX_VALUE));
    assertThat(val.getTupleValue(1).getShort(0)).isEqualTo(Short.MAX_VALUE);
    assertThat(val.getTupleValue(1).getFloat(1)).isEqualTo(Float.MAX_VALUE);

    val =
        complexTypes
            .column("tuple")
            .type()
            .create(
                Instant.now(),
                Type.Tuple.of(Type.Smallint, Type.Float)
                    .create(Short.MAX_VALUE, BigDecimal.valueOf(Float.MAX_VALUE)));

    assertThat(val.getTupleValue(1).getShort(0)).isEqualTo(Short.MAX_VALUE);
    assertThat(val.getTupleValue(1).getFloat(1)).isEqualTo(Float.MAX_VALUE);
  }

  @Test
  public void wideningAndNarrowingForUdt() {
    Schema schema =
        Schema.build()
            .keyspace("test")
            .type("address")
            .column("a", Type.Smallint)
            .column("b", Type.Float)
            .table("complexTypes")
            .column("udt", UserDefinedType.reference("address"), PartitionKey)
            .build();

    UdtValue udtValue =
        schema
            .keyspace("test")
            .userDefinedType("address")
            .create(BigInteger.valueOf(Short.MAX_VALUE), Float.MAX_VALUE);

    assertThat(udtValue.getShort("a")).isEqualTo(Short.MAX_VALUE);
    assertThat(udtValue.getFloat("b")).isEqualTo(Float.MAX_VALUE);

    udtValue =
        schema
            .keyspace("test")
            .userDefinedType("address")
            .create(Short.MAX_VALUE, BigDecimal.valueOf(Float.MAX_VALUE));
    assertThat(udtValue.getShort("a")).isEqualTo(Short.MAX_VALUE);
    assertThat(udtValue.getFloat("b")).isEqualTo(Float.MAX_VALUE);
  }

  @Test
  public void testComplexSubtypesAreFrozenByDefault() {
    Schema schemaDef =
        Schema.build()
            .keyspace("test")
            .type("udt1")
            .column("a", Type.Int)
            .column("b", Type.Text)
            .type("udt2")
            .column("a", Type.Int)
            .column("b", Type.Text)
            .column("nested_udt", UserDefinedType.reference("udt1"))
            .column("nested_list", Type.List.of(Type.Int))
            .column("nested_set", Type.Set.of(Type.Int))
            .column("nested_map", Type.Map.of(Type.Int, Type.Int))
            .column("nested_tuple", Type.Tuple.of(Type.Bigint))
            .table("tbl")
            .column("id", Type.Int, PartitionKey)
            .column("listOfLists", Type.List.of(Type.List.of(Type.Double)))
            .column("setOfSets", Type.Set.of(Type.Set.of(Type.Float)))
            .column("mapOfLists", Type.Map.of(Type.Int, Type.List.of(Type.Int)))
            .column("udt", UserDefinedType.reference("udt2"))
            .column(
                "tuple",
                Type.Tuple.of(
                    Type.List.of(Type.Int),
                    Type.Set.of(Type.Int),
                    Type.Map.of(Type.Int, Type.Int),
                    Type.Tuple.of(Type.Bigint, Type.List.of(Type.Bigint)),
                    UserDefinedType.reference("udt2")))
            .build();

    Keyspace keyspace = schemaDef.keyspace("test");
    assertThat(keyspace.userDefinedType("udt1").isFrozen()).isFalse();

    UserDefinedType udt = keyspace.userDefinedType("udt2");
    assertThat(udt.isFrozen()).isFalse();
    assertThat(udt.columnMap().get("a").type().isFrozen()).isFalse();
    assertThat(udt.columnMap().get("b").type().isFrozen()).isFalse();
    assertThat(udt.columnMap().get("nested_udt").type().isFrozen()).isTrue();
    assertThat(udt.columnMap().get("nested_list").type().isFrozen()).isTrue();
    assertThat(udt.columnMap().get("nested_set").type().isFrozen()).isTrue();
    assertThat(udt.columnMap().get("nested_map").type().isFrozen()).isTrue();
    assertThat(udt.columnMap().get("nested_tuple").type().isFrozen()).isTrue();

    Table tbl = keyspace.table("tbl");
    assertThat(tbl.column("listOfLists").type().isFrozen()).isFalse();
    assertThat(tbl.column("listOfLists").type().parameters().get(0).isFrozen()).isTrue();

    assertThat(tbl.column("setOfSets").type().isFrozen()).isFalse();
    assertThat(tbl.column("setOfSets").type().parameters().get(0).isFrozen()).isTrue();

    assertThat(tbl.column("mapOfLists").type().isFrozen()).isFalse();
    assertThat(tbl.column("mapOfLists").type().parameters().get(0).isFrozen()).isFalse();
    assertThat(tbl.column("mapOfLists").type().parameters().get(1).isFrozen()).isTrue();

    ParameterizedType.TupleType tuple = (ParameterizedType.TupleType) tbl.column("tuple").type();
    // Tuples are frozen by default anyway
    assertThat(tuple.isFrozen()).isTrue();
    assertThat(tuple.parameterMap().get("field1").isFrozen()).isTrue();
    assertThat(tuple.parameterMap().get("field2").isFrozen()).isTrue();
    assertThat(tuple.parameterMap().get("field3").isFrozen()).isTrue();
    assertThat(tuple.parameterMap().get("field4").isFrozen()).isTrue();
    assertThat(tuple.parameterMap().get("field5").isFrozen()).isTrue();
  }
}
