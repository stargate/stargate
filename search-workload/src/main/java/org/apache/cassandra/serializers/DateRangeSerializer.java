/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.serializers;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Responsible for {@link DateRange} serialization/deserialization with respect to the following
 * format: ------------------------- <type>[<time0><precision0>[<time1><precision1>]]
 *
 * <p>Where:
 *
 * <p><type> is a [byte] encoding of - 0x00 - single value as in "2001-01-01" - 0x01 - closed range
 * as in "[2001-01-01 TO 2001-01-31]" - 0x02 - open range high as in "[2001-01-01 TO *]" - 0x03 -
 * open range low as in "[* TO 2001-01-01]" - 0x04 - both ranges open as in "[* TO *]" - 0x05 -
 * single value open as in "*"
 *
 * <p><time0> is an optional [long] millisecond offset from epoch. Absent for <type> in [4,5],
 * present otherwise. Represents a single date value for <type> = 0, the range start for <type> in
 * [1,2], or range end for <type> = 3.
 *
 * <p><precision0> is an optional [byte]s and represents the precision of field <time0>. Absent for
 * <type> in [4,5], present otherwise. Possible values are: - 0x00 - year - 0x01 - month - 0x02 -
 * day - 0x03 - hour - 0x04 - minute - 0x05 - second - 0x06 - millisecond
 *
 * <p><time1> is an optional [long] millisecond offset from epoch. Represents the range end for
 * <type> = 1. Not present otherwise.
 *
 * <p><precision1> is an optional [byte] and represents the precision of field <time1>. Only present
 * if <type> = 1. Values are the same as for <precision0>.
 */
public final class DateRangeSerializer implements TypeSerializer<DateRange> {
  public static final DateRangeSerializer instance = new DateRangeSerializer();

  // e.g. [2001-01-01]
  private static final byte DATE_RANGE_TYPE_SINGLE_DATE = 0x00;
  // e.g. [2001-01-01 TO 2001-01-31]
  private static final byte DATE_RANGE_TYPE_CLOSED_RANGE = 0x01;
  // e.g. [2001-01-01 TO *]
  private static final byte DATE_RANGE_TYPE_OPEN_RANGE_HIGH = 0x02;
  // e.g. [* TO 2001-01-01]
  private static final byte DATE_RANGE_TYPE_OPEN_RANGE_LOW = 0x03;
  // [* TO *]
  private static final byte DATE_RANGE_TYPE_BOTH_OPEN_RANGE = 0x04;
  // *
  private static final byte DATE_RANGE_TYPE_SINGLE_DATE_OPEN = 0x05;

  private static final List<Integer> VALID_SERIALIZED_LENGTHS =
      ImmutableList.of(
          // types: 0x04, 0x05
          Byte.BYTES,
          // types: 0x00, 0x02, 0x03
          Byte.BYTES + Long.BYTES + Byte.BYTES,
          // types: 0x01
          Byte.BYTES + Long.BYTES + Byte.BYTES + Long.BYTES + Byte.BYTES);

  @Override
  public ByteBuffer serialize(DateRange dateRange) {
    if (dateRange == null) {
      return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    byte rangeType = encodeType(dateRange);

    int bufferSize = 1;
    if (!dateRange.getLowerBound().isUnbounded()) {
      bufferSize += 9;
    }
    if (dateRange.isUpperBoundDefined() && !dateRange.getUpperBound().isUnbounded()) {
      bufferSize += 9;
    }

    try (DataOutputBuffer output = new DataOutputBuffer(bufferSize)) {
      output.writeByte(rangeType);
      DateRange.DateRangeBound lowerBound = dateRange.getLowerBound();
      if (!lowerBound.isUnbounded()) {
        output.writeLong(lowerBound.getTimestamp().toEpochMilli());
        output.writeByte(lowerBound.getPrecision().toEncoded());
      }

      if (dateRange.isUpperBoundDefined()) {
        DateRange.DateRangeBound upperBound = dateRange.getUpperBound();
        if (!upperBound.isUnbounded()) {
          output.writeLong(upperBound.getTimestamp().toEpochMilli());
          output.writeByte(upperBound.getPrecision().toEncoded());
        }
      }
      return output.buffer();
    } catch (IOException e) {
      throw new AssertionError("Unexpected error", e);
    }
  }

  @Override
  public DateRange deserialize(ByteBuffer bytes) {
    if (bytes.remaining() == 0) {
      return null;
    }

    try (DataInputBuffer input = new DataInputBuffer(bytes, true)) {
      byte type = input.readByte();
      switch (type) {
        case DATE_RANGE_TYPE_SINGLE_DATE:
          return new DateRange(deserializeDateRangeLowerBound(input));
        case DATE_RANGE_TYPE_CLOSED_RANGE:
          return new DateRange(
              deserializeDateRangeLowerBound(input), deserializeDateRangeUpperBound(input));
        case DATE_RANGE_TYPE_OPEN_RANGE_HIGH:
          return new DateRange(
              deserializeDateRangeLowerBound(input), DateRange.DateRangeBound.UNBOUNDED);
        case DATE_RANGE_TYPE_OPEN_RANGE_LOW:
          return new DateRange(
              DateRange.DateRangeBound.UNBOUNDED, deserializeDateRangeUpperBound(input));
        case DATE_RANGE_TYPE_BOTH_OPEN_RANGE:
          return new DateRange(
              DateRange.DateRangeBound.UNBOUNDED, DateRange.DateRangeBound.UNBOUNDED);
        case DATE_RANGE_TYPE_SINGLE_DATE_OPEN:
          return new DateRange(DateRange.DateRangeBound.UNBOUNDED);
        default:
          throw new IllegalArgumentException("Unknown date range type: " + type);
      }
    } catch (IOException e) {
      throw new AssertionError("Unexpected error", e);
    }
  }

  @Override
  public void validate(ByteBuffer bytes) throws MarshalException {
    if (bytes.remaining() == 0) {
      return;
    } else if (!VALID_SERIALIZED_LENGTHS.contains(bytes.remaining())) {
      throw new MarshalException(
          String.format(
              "Date range should be have %s bytes, got %d instead.",
              VALID_SERIALIZED_LENGTHS, bytes.remaining()));
    }
    DateRange dateRange = deserialize(bytes);
    validateDateRange(dateRange);
  }

  @Override
  public String toString(DateRange dateRange) {
    return dateRange == null ? "" : dateRange.formatToSolrString();
  }

  @Override
  public Class<DateRange> getType() {
    return DateRange.class;
  }

  private byte encodeType(DateRange dateRange) {
    if (dateRange.isUpperBoundDefined()) {
      if (dateRange.getLowerBound().isUnbounded()) {
        return dateRange.getUpperBound().isUnbounded()
            ? DATE_RANGE_TYPE_BOTH_OPEN_RANGE
            : DATE_RANGE_TYPE_OPEN_RANGE_LOW;
      } else {
        return dateRange.getUpperBound().isUnbounded()
            ? DATE_RANGE_TYPE_OPEN_RANGE_HIGH
            : DATE_RANGE_TYPE_CLOSED_RANGE;
      }
    } else {
      return dateRange.getLowerBound().isUnbounded()
          ? DATE_RANGE_TYPE_SINGLE_DATE_OPEN
          : DATE_RANGE_TYPE_SINGLE_DATE;
    }
  }

  private DateRange.DateRangeBound deserializeDateRangeLowerBound(DataInputBuffer input)
      throws IOException {
    long epochMillis = input.readLong();
    Precision precision = Precision.fromEncoded(input.readByte());
    return DateRange.DateRangeBound.lowerBound(Instant.ofEpochMilli(epochMillis), precision);
  }

  private DateRange.DateRangeBound deserializeDateRangeUpperBound(DataInputBuffer input)
      throws IOException {
    long epochMillis = input.readLong();
    Precision precision = Precision.fromEncoded(input.readByte());
    return DateRange.DateRangeBound.upperBound(Instant.ofEpochMilli(epochMillis), precision);
  }

  private void validateDateRange(DateRange dateRange) {
    if (!dateRange.getLowerBound().isUnbounded()
        && dateRange.isUpperBoundDefined()
        && !dateRange.getUpperBound().isUnbounded()) {
      if (dateRange
          .getLowerBound()
          .getTimestamp()
          .isAfter(dateRange.getUpperBound().getTimestamp())) {
        throw new MarshalException(
            String.format(
                "Lower bound of a date range should be before upper bound, got: %s",
                dateRange.formatToSolrString()));
      }
    }
  }
}
