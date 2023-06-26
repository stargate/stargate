/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.schema;

import static io.stargate.db.schema.NumberCoercion.coerceToColumnType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class NumberCoercionTest {
  @Test
  public void nullValue() {
    assertThat(coerceToColumnType(BigInteger.class, null).columnTypeAcceptsValue()).isFalse();
  }

  @Test
  public void valueNotANumber() {
    assertThat(coerceToColumnType(BigInteger.class, "23").columnTypeAcceptsValue()).isFalse();
  }

  @Test
  public void nullClass() {
    assertThat(coerceToColumnType(null, Long.MAX_VALUE).columnTypeAcceptsValue()).isFalse();
  }

  @Test
  public void decimalsToNaturalNumbers() {
    assertThat(
            coerceToColumnType(BigInteger.class, BigDecimal.valueOf(Double.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isTrue();
    assertThat(
            coerceToColumnType(BigInteger.class, BigDecimal.valueOf(1.000001d))
                .columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(BigInteger.class, Double.MAX_VALUE).columnTypeAcceptsValue())
        .isTrue();
    assertThat(coerceToColumnType(BigInteger.class, 1.000000d).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(BigInteger.class, 1.000001d).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(BigInteger.class, Float.MAX_VALUE).columnTypeAcceptsValue())
        .isTrue();
    assertThat(coerceToColumnType(BigInteger.class, 1.000001f).columnTypeAcceptsValue()).isFalse();

    assertThat(
            coerceToColumnType(Long.class, BigDecimal.valueOf(Long.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isTrue();
    assertThat(
            coerceToColumnType(Long.class, BigDecimal.valueOf(1.000001d)).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Long.class, Double.MAX_VALUE).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Long.class, 1.000000d).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Long.class, 1.000001d).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Long.class, Float.MAX_VALUE).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Long.class, 1.000001f).columnTypeAcceptsValue()).isFalse();

    assertThat(
            coerceToColumnType(Integer.class, BigDecimal.valueOf(Integer.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isTrue();
    assertThat(
            coerceToColumnType(Integer.class, BigDecimal.valueOf(1.000001d))
                .columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Integer.class, 1.000000d).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Integer.class, 1.000001d).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Integer.class, Float.MAX_VALUE).columnTypeAcceptsValue())
        .isTrue();
    assertThat(coerceToColumnType(Integer.class, 1.000001f).columnTypeAcceptsValue()).isFalse();

    assertThat(
            coerceToColumnType(Short.class, BigDecimal.valueOf(Short.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isTrue();
    assertThat(
            coerceToColumnType(Short.class, BigDecimal.valueOf(1.000001d)).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Short.class, 1.000000d).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Short.class, 1.000001d).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Short.class, 1.000000f).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Short.class, 1.000001f).columnTypeAcceptsValue()).isFalse();

    assertThat(
            coerceToColumnType(Byte.class, BigDecimal.valueOf(Byte.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isTrue();
    assertThat(
            coerceToColumnType(Byte.class, BigDecimal.valueOf(1.000001d)).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Byte.class, 1.000000d).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Byte.class, 1.000001d).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Byte.class, 1.000000f).columnTypeAcceptsValue()).isTrue();
    assertThat(coerceToColumnType(Byte.class, 1.000001f).columnTypeAcceptsValue()).isFalse();
  }

  @Test
  public void naturalNumbersToDecimals() {
    assertThat(
            coerceToColumnType(BigDecimal.class, BigInteger.valueOf(Long.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(BigDecimal.class, Long.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(BigDecimal.class, Integer.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(BigDecimal.class, Short.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(BigDecimal.class, Byte.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();

    assertThat(
            coerceToColumnType(Double.class, BigInteger.valueOf(Long.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Double.class, Long.MAX_VALUE).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Double.class, Integer.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Double.class, Short.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Double.class, Byte.MAX_VALUE).columnTypeAcceptsValue()).isFalse();

    assertThat(
            coerceToColumnType(Float.class, BigInteger.valueOf(Long.MAX_VALUE))
                .columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Float.class, Long.MAX_VALUE).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Float.class, Integer.MAX_VALUE).columnTypeAcceptsValue())
        .isFalse();
    assertThat(coerceToColumnType(Float.class, Short.MAX_VALUE).columnTypeAcceptsValue()).isFalse();
    assertThat(coerceToColumnType(Float.class, Byte.MAX_VALUE).columnTypeAcceptsValue()).isFalse();
  }

  @Test
  public void wideningToBigInteger() {
    BigInteger exp = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    NumberCoercion.Result result = coerceToColumnType(BigInteger.class, exp);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(exp);

    result = coerceToColumnType(BigInteger.class, Long.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(Long.MAX_VALUE));

    result = coerceToColumnType(BigInteger.class, Integer.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(Integer.MAX_VALUE));

    result = coerceToColumnType(BigInteger.class, Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(Short.MAX_VALUE));

    result = coerceToColumnType(BigInteger.class, Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(Byte.MAX_VALUE));

    result = coerceToColumnType(BigInteger.class, BigDecimal.valueOf(1.000000d));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(1));

    result = coerceToColumnType(BigInteger.class, 1.000000d);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(1));

    result = coerceToColumnType(BigInteger.class, 1.000000f);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigInteger.valueOf(1));
  }

  @Test
  public void wideningAndNarrowingToLong() {
    NumberCoercion.Result result = coerceToColumnType(Long.class, Long.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Long.MAX_VALUE);

    result = coerceToColumnType(Long.class, Integer.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((long) Integer.MAX_VALUE);

    result = coerceToColumnType(Long.class, Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((long) Short.MAX_VALUE);

    result = coerceToColumnType(Long.class, Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((long) Byte.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Long.class, BigInteger.valueOf(Long.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Long.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Long.class, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigInteger out of long range");

    result = coerceToColumnType(Long.class, BigDecimal.valueOf(1.000000d));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1L);

    result = coerceToColumnType(Long.class, 1.000000d);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1L);

    result = coerceToColumnType(Long.class, 1.000000f);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1L);
  }

  @Test
  public void wideningAndNarrowingToInt() {
    NumberCoercion.Result result = coerceToColumnType(Integer.class, Integer.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Integer.MAX_VALUE);

    result = coerceToColumnType(Integer.class, Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((int) Short.MAX_VALUE);

    result = coerceToColumnType(Integer.class, Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((int) Byte.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Integer.class, BigInteger.valueOf(Integer.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Integer.MAX_VALUE);

    result = coerceToColumnType(Integer.class, (long) Integer.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Integer.MAX_VALUE);

    result = coerceToColumnType(Integer.class, BigDecimal.valueOf(Integer.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Integer.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Integer.class, BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigInteger out of int range");

    assertThatThrownBy(() -> coerceToColumnType(Integer.class, (long) Integer.MAX_VALUE + 1L))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long out of int range");

    result = coerceToColumnType(Integer.class, BigDecimal.valueOf(1.000000d));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1);

    result = coerceToColumnType(Integer.class, 1.000000d);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1);

    result = coerceToColumnType(Integer.class, 1.000000f);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(1);
  }

  @Test
  public void wideningAndNarrowingToShort() {
    NumberCoercion.Result result = coerceToColumnType(Short.class, Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Short.MAX_VALUE);

    result = coerceToColumnType(Short.class, Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((short) Byte.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Short.class, BigInteger.valueOf(Short.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Short.MAX_VALUE);

    result = coerceToColumnType(Short.class, (long) Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Short.MAX_VALUE);

    result = coerceToColumnType(Short.class, (int) Short.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Short.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Short.class, BigInteger.valueOf(Short.MAX_VALUE).add(BigInteger.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigInteger out of short range");

    assertThatThrownBy(() -> coerceToColumnType(Short.class, (long) Short.MAX_VALUE + 1L))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long out of short range");

    assertThatThrownBy(() -> coerceToColumnType(Short.class, (int) Short.MAX_VALUE + 1))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("int out of short range");

    result = coerceToColumnType(Short.class, BigDecimal.valueOf(1.000000d));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((short) 1);

    result = coerceToColumnType(Short.class, 1.000000d);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((short) 1);

    result = coerceToColumnType(Short.class, 1.000000f);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((short) 1);
  }

  @Test
  public void wideningAndNarrowingToByte() {
    NumberCoercion.Result result = coerceToColumnType(Byte.class, Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Byte.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Byte.class, BigInteger.valueOf(Byte.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Byte.MAX_VALUE);

    result = coerceToColumnType(Byte.class, (long) Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Byte.MAX_VALUE);

    result = coerceToColumnType(Byte.class, (int) Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Byte.MAX_VALUE);

    result = coerceToColumnType(Byte.class, (short) Byte.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Byte.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Byte.class, BigInteger.valueOf(Byte.MAX_VALUE).add(BigInteger.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigInteger out of byte range");

    assertThatThrownBy(() -> coerceToColumnType(Byte.class, (long) Byte.MAX_VALUE + 1L))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long out of byte range");

    assertThatThrownBy(() -> coerceToColumnType(Byte.class, (int) Byte.MAX_VALUE + 1))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("int out of byte range");

    assertThatThrownBy(() -> coerceToColumnType(Byte.class, (short) (Byte.MAX_VALUE + 1)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("short out of byte range");

    result = coerceToColumnType(Byte.class, BigDecimal.valueOf(1.000000d));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((byte) 1);

    result = coerceToColumnType(Byte.class, 1.000000d);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((byte) 1);

    result = coerceToColumnType(Byte.class, 1.000000f);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((byte) 1);
  }

  @Test
  public void wideningToBigDecimal() {
    BigDecimal exp = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE);
    NumberCoercion.Result result = coerceToColumnType(BigDecimal.class, exp);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(exp);

    result = coerceToColumnType(BigDecimal.class, Double.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE));

    result = coerceToColumnType(BigDecimal.class, Float.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(BigDecimal.valueOf(Float.MAX_VALUE));
  }

  @Test
  public void wideningAndNarrowingToDouble() {
    NumberCoercion.Result result = coerceToColumnType(Double.class, Double.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Double.MAX_VALUE);

    result = coerceToColumnType(Double.class, Float.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo((double) Float.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Double.class, BigDecimal.valueOf(Double.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Double.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Double.class, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigDecimal out of double range");
  }

  @Test
  public void wideningAndNarrowingToFloat() {
    NumberCoercion.Result result = coerceToColumnType(Float.class, Float.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Float.MAX_VALUE);

    // narrowing in range
    result = coerceToColumnType(Float.class, BigDecimal.valueOf(Float.MAX_VALUE));
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Float.MAX_VALUE);

    result = coerceToColumnType(Float.class, (double) Float.MAX_VALUE);
    assertThat(result.columnTypeAcceptsValue()).isTrue();
    assertThat(result.getValidatedValue()).isEqualTo(Float.MAX_VALUE);

    assertThatThrownBy(
            () ->
                coerceToColumnType(
                    Float.class, BigDecimal.valueOf(Float.MAX_VALUE).add(BigDecimal.ONE)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("BigDecimal out of float range");

    assertThatThrownBy(() -> coerceToColumnType(Float.class, Double.MAX_VALUE))
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("double out of float range");
  }
}
