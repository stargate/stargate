/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.schema;

import static io.stargate.db.schema.NumberCoercion.Result.coercionFailed;
import static io.stargate.db.schema.NumberCoercion.Result.coercionOk;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * We're doing lots of boxing/unboxing here, which is inefficient. This type of validation will be
 * done for every insert of a value, so we should measure performance and improve.
 */
class NumberCoercion {
  private static byte toByteExact(short value) {
    byte narrowed = (byte) value;
    if (narrowed != value) {
      throw new ArithmeticException("short out of byte range");
    }
    return narrowed;
  }

  private static byte toByteExact(int value) {
    byte narrowed = (byte) value;
    if (narrowed != value) {
      throw new ArithmeticException("int out of byte range");
    }
    return narrowed;
  }

  private static byte toByteExact(long value) {
    byte narrowed = (byte) value;
    if (narrowed != value) {
      throw new ArithmeticException("long out of byte range");
    }
    return narrowed;
  }

  private static byte toByteExact(double value) {
    byte narrowed = (byte) value;
    if (narrowed != value) {
      throw new ArithmeticException("double out of byte range");
    }
    return narrowed;
  }

  private static byte toByteExact(float value) {
    byte narrowed = (byte) value;
    if (narrowed != value) {
      throw new ArithmeticException("float out of byte range");
    }
    return narrowed;
  }

  private static short toShortExact(int value) {
    short narrowed = (short) value;
    if (narrowed != value) {
      throw new ArithmeticException("int out of short range");
    }
    return narrowed;
  }

  private static short toShortExact(long value) {
    short narrowed = (short) value;
    if (narrowed != value) {
      throw new ArithmeticException("long out of short range");
    }
    return narrowed;
  }

  private static short toShortExact(double value) {
    short narrowed = (short) value;
    if (narrowed != value) {
      throw new ArithmeticException("double out of short range");
    }
    return narrowed;
  }

  private static short toShortExact(float value) {
    short narrowed = (short) value;
    if (narrowed != value) {
      throw new ArithmeticException("float out of short range");
    }
    return narrowed;
  }

  private static int toIntExact(long value) {
    int narrowed = (int) value;
    if (narrowed != value) {
      throw new ArithmeticException("long out of int range");
    }
    return narrowed;
  }

  private static float toFloatExact(double value) {
    float narrowed = (float) value;
    if (Double.compare(narrowed, value) != 0) {
      throw new ArithmeticException("double out of float range");
    }
    return narrowed;
  }

  private static float toFloatExact(BigDecimal value) {
    float narrowed = value.floatValue();
    if (BigDecimal.valueOf(narrowed).compareTo(value) != 0) {
      throw new ArithmeticException("BigDecimal out of float range");
    }
    return narrowed;
  }

  private static double toDoubleExact(BigDecimal value) {
    double narrowed = value.doubleValue();

    if (BigDecimal.valueOf(narrowed).compareTo(value) != 0) {
      throw new ArithmeticException("BigDecimal out of double range");
    }
    return narrowed;
  }

  private static boolean isFloat(Object value) {
    return value instanceof Float;
  }

  private static boolean isDouble(Object value) {
    return value instanceof Double;
  }

  private static boolean isBigDecimal(Object value) {
    return value instanceof BigDecimal;
  }

  private static boolean isByte(Object value) {
    return value instanceof Byte;
  }

  private static boolean isShort(Object value) {
    return value instanceof Short;
  }

  private static boolean isInt(Object value) {
    return value instanceof Integer;
  }

  private static boolean isLong(Object value) {
    return value instanceof Long;
  }

  private static boolean isBigInt(Object value) {
    return value instanceof BigInteger;
  }

  private static BigInteger toBigInteger(Object value) {
    if (isBigInt(value)) {
      return (BigInteger) value;
    }
    if (isBigDecimal(value)) {
      return ((BigDecimal) value).toBigIntegerExact();
    }
    if (isLong(value)) {
      return BigInteger.valueOf((long) value);
    }
    if (isDouble(value)) {
      return BigInteger.valueOf(((Double) value).longValue());
    }
    if (isInt(value)) {
      return BigInteger.valueOf((int) value);
    }
    if (isFloat(value)) {
      return BigInteger.valueOf(((Float) value).longValue());
    }
    if (isShort(value)) {
      return BigInteger.valueOf((short) value);
    }
    return BigInteger.valueOf((byte) value);
  }

  private static long toLong(Object value) {
    if (isBigInt(value)) {
      return ((BigInteger) value).longValueExact();
    }
    if (isBigDecimal(value)) {
      return ((BigDecimal) value).longValueExact();
    }
    if (isLong(value)) {
      return (long) value;
    }
    if (isDouble(value)) {
      return ((Double) value).longValue();
    }
    if (isInt(value)) {
      return (int) value;
    }
    if (isFloat(value)) {
      return ((Float) value).longValue();
    }
    if (isShort(value)) {
      return (short) value;
    }
    return (byte) value;
  }

  private static int toInt(Object value) {
    if (isBigInt(value)) {
      return ((BigInteger) value).intValueExact();
    }
    if (isBigDecimal(value)) {
      return ((BigDecimal) value).intValueExact();
    }
    if (isLong(value)) {
      return toIntExact((long) value);
    }
    if (isDouble(value)) {
      return ((Double) value).intValue();
    }
    if (isInt(value)) {
      return (int) value;
    }
    if (isFloat(value)) {
      return ((Float) value).intValue();
    }
    if (isShort(value)) {
      return (short) value;
    }
    return (byte) value;
  }

  private static short toShort(Object value) {
    if (isBigInt(value)) {
      return ((BigInteger) value).shortValueExact();
    }
    if (isBigDecimal(value)) {
      return ((BigDecimal) value).shortValueExact();
    }
    if (isLong(value)) {
      return toShortExact((long) value);
    }
    if (isDouble(value)) {
      return toShortExact((double) value);
    }
    if (isInt(value)) {
      return toShortExact((int) value);
    }
    if (isFloat(value)) {
      return toShortExact((float) value);
    }
    if (isShort(value)) {
      return (short) value;
    }
    return (byte) value;
  }

  private static byte toByte(Object value) {
    if (isBigInt(value)) {
      return ((BigInteger) value).byteValueExact();
    }
    if (isBigDecimal(value)) {
      return ((BigDecimal) value).byteValueExact();
    }
    if (isLong(value)) {
      return toByteExact((long) value);
    }
    if (isDouble(value)) {
      return toByteExact((double) value);
    }
    if (isInt(value)) {
      return toByteExact((int) value);
    }
    if (isFloat(value)) {
      return toByteExact((float) value);
    }
    if (isShort(value)) {
      return toByteExact((short) value);
    }
    return (byte) value;
  }

  private static BigDecimal toBigDecimal(Object value) {
    if (isBigDecimal(value)) {
      return (BigDecimal) value;
    }
    if (isDouble(value)) {
      return BigDecimal.valueOf((double) value);
    }
    return BigDecimal.valueOf((float) value);
  }

  private static double toDouble(Object value) {
    if (isBigDecimal(value)) {
      return toDoubleExact((BigDecimal) value);
    }
    if (isDouble(value)) {
      return (double) value;
    }
    return (float) value;
  }

  private static float toFloat(Object value) {
    if (isBigDecimal(value)) {
      return toFloatExact((BigDecimal) value);
    } else if (isDouble(value)) {
      return toFloatExact((double) value);
    }
    return (float) value;
  }

  private static boolean isDecimalNumber(Object value) {
    return (isBigDecimal(value) && ((BigDecimal) value).stripTrailingZeros().scale() > 0)
        || (isDouble(value) && ((double) value % 1 > 0))
        || (isFloat(value) && ((float) value % 1 > 0));
  }

  private static boolean isNaturalNumber(Object value) {
    return isBigInt(value) || isLong(value) || isInt(value) || isShort(value) || isByte(value);
  }

  /**
   * Checks if the given columnType class can accept the given value using custom <b>Narrowing &
   * Widening rules</b>.
   *
   * <p>Please note that Narrowing rules are different from the <b>JLS - 5.1.3 Narrowing Primitive
   * Conversions</b>.
   *
   * <p>The general rule is that narrowing & widening is done only for
   * <li>1. Natural numbers: BigInteger > Long > Integer > Short > Byte
   * <li>2. Decimal numbers: BigDecimal > Double > Float Narrowing & widening e.g. a BigDecimal to
   *     an Integer is not supported and will return <code>false</code>.
   *
   * @param columnType The underlying java type of a column
   * @param value The value to be applied to the given columnType
   * @return A {@link Result} object with <code>true</code> if the given columnType can accept the
   *     given value using the above described Narrowing & Widening rules, <code>false</code>
   *     otherwise. {@link Result} also contains the narrowed/widened value.
   * @throws ArithmeticException in case a value gets out of range when narrowing it to a lower
   *     type.
   */
  public static Result coerceToColumnType(Class<?> columnType, Object value) {
    if (!(value instanceof Number)) {
      return coercionFailed(value);
    }

    if (columnTypeRequiresNaturalNumber(columnType) && isDecimalNumber(value)) {
      return coercionFailed(value);
    }

    if (columnTypeRequiresDecimalNumber(columnType) && isNaturalNumber(value)) {
      return coercionFailed(value);
    }

    if (columnTypeIsBigInteger(columnType)) {
      return coercionOk(toBigInteger(value));
    }

    if (columnTypeIsLong(columnType)) {
      return coercionOk(toLong(value));
    }

    if (columnTypeIsInt(columnType)) {
      return coercionOk(toInt(value));
    }

    if (columnTypeIsShort(columnType)) {
      return coercionOk(toShort(value));
    }

    if (columnTypeIsByte(columnType)) {
      return coercionOk(toByte(value));
    }

    if (columnTypeIsBigDecimal(columnType)) {
      return coercionOk(toBigDecimal(value));
    }

    if (columnTypeIsDouble(columnType)) {
      return coercionOk(toDouble(value));
    }

    if (columnTypeIsFloat(columnType)) {
      return coercionOk(toFloat(value));
    }
    return coercionFailed(value);
  }

  private static boolean columnTypeRequiresNaturalNumber(Class<?> columnType) {
    return columnTypeIsBigInteger(columnType)
        || columnTypeIsLong(columnType)
        || columnTypeIsInt(columnType)
        || columnTypeIsShort(columnType)
        || columnTypeIsByte(columnType);
  }

  private static boolean columnTypeRequiresDecimalNumber(Class<?> columnType) {
    return columnTypeIsBigDecimal(columnType)
        || columnTypeIsDouble(columnType)
        || columnTypeIsFloat(columnType);
  }

  private static boolean columnTypeIsFloat(Class<?> columnType) {
    return Float.class.equals(columnType);
  }

  private static boolean columnTypeIsDouble(Class<?> columnType) {
    return Double.class.equals(columnType);
  }

  private static boolean columnTypeIsBigDecimal(Class<?> columnType) {
    return BigDecimal.class.equals(columnType);
  }

  private static boolean columnTypeIsByte(Class<?> columnType) {
    return Byte.class.equals(columnType);
  }

  private static boolean columnTypeIsShort(Class<?> columnType) {
    return Short.class.equals(columnType);
  }

  private static boolean columnTypeIsInt(Class<?> columnType) {
    return Integer.class.equals(columnType);
  }

  private static boolean columnTypeIsLong(Class<?> columnType) {
    return Long.class.equals(columnType);
  }

  private static boolean columnTypeIsBigInteger(Class<?> columnType) {
    return BigInteger.class.equals(columnType);
  }

  static class Result {
    final boolean columnTypeAcceptsValue;
    final Object validatedValue;

    Result(boolean columnTypeAcceptsValue, Object validatedValue) {
      this.columnTypeAcceptsValue = columnTypeAcceptsValue;
      this.validatedValue = validatedValue;
    }

    public static Result coercionFailed(Object validatedValue) {
      return new Result(false, validatedValue);
    }

    public static Result coercionOk(Object validatedValue) {
      return new Result(true, validatedValue);
    }

    public boolean columnTypeAcceptsValue() {
      return columnTypeAcceptsValue;
    }

    public Object getValidatedValue() {
      return validatedValue;
    }
  }
}
