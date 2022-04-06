/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal.datetime;

import static java.time.temporal.TemporalAdjusters.*;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;

public class DateRangeUtil {
  private static final DateRangePrefixTree prefixTree = DateRangePrefixTree.INSTANCE;

  public static DateRange parseDateRange(String source) throws ParseException {
    if (StringUtils.isBlank(source)) {
      throw new IllegalArgumentException("Date range is null or blank");
    }
    if (source.charAt(0) == '[') {
      if (source.charAt(source.length() - 1) != ']') {
        throw new IllegalArgumentException(
            "If date range starts with [ must end with ]; got " + source);
      }
      int middle = source.indexOf(" TO ");
      if (middle < 0) {
        throw new IllegalArgumentException(
            "If date range starts with [ must contain ' TO '; got " + source);
      }
      String lowerBoundString = source.substring(1, middle);
      String upperBoundString = source.substring(middle + " TO ".length(), source.length() - 1);
      return new DateRange(parseLowerBound(lowerBoundString), parseUpperBound(upperBoundString));
    } else {
      return new DateRange(parseLowerBound(source));
    }
  }

  public static ZonedDateTime roundUpperBoundTimestampToPrecision(
      ZonedDateTime timestamp, DateRangeBound.Precision precision) {
    switch (precision) {
      case YEAR:
        timestamp = timestamp.with(lastDayOfYear());
      case MONTH:
        timestamp = timestamp.with(lastDayOfMonth());
      case DAY:
        timestamp = timestamp.with(ChronoField.HOUR_OF_DAY, 23);
      case HOUR:
        timestamp = timestamp.with(ChronoField.MINUTE_OF_HOUR, 59);
      case MINUTE:
        timestamp = timestamp.with(ChronoField.SECOND_OF_MINUTE, 59);
      case SECOND:
        timestamp = timestamp.with(ChronoField.MILLI_OF_SECOND, 999);
      case MILLISECOND:
        // DateRangeField ignores any precision beyond milliseconds
        return timestamp;
      default:
        throw new IllegalStateException(
            "Unsupported date time precision for the upper bound: " + precision);
    }
  }

  public static ZonedDateTime roundLowerBoundTimestampToPrecision(
      ZonedDateTime timestamp, DateRangeBound.Precision precision) {
    switch (precision) {
      case YEAR:
        timestamp = timestamp.with(firstDayOfYear());
      case MONTH:
        timestamp = timestamp.with(firstDayOfMonth());
      case DAY:
        timestamp = timestamp.with(ChronoField.HOUR_OF_DAY, 0);
      case HOUR:
        timestamp = timestamp.with(ChronoField.MINUTE_OF_HOUR, 0);
      case MINUTE:
        timestamp = timestamp.with(ChronoField.SECOND_OF_MINUTE, 0);
      case SECOND:
        timestamp = timestamp.with(ChronoField.MILLI_OF_SECOND, 0);
      case MILLISECOND:
        // DateRangeField ignores any precision beyond milliseconds
        return timestamp;
      default:
        throw new IllegalStateException(
            "Unsupported date time precision for the upper bound: " + precision);
    }
  }

  private static DateRangeBound parseLowerBound(String source) throws ParseException {
    Calendar lowerBoundCalendar = prefixTree.parseCalendar(source);
    int calPrecisionField = prefixTree.getCalPrecisionField(lowerBoundCalendar);
    if (calPrecisionField < 0) {
      return DateRangeBound.UNBOUNDED;
    }
    return DateRangeBound.lowerBound(
        toZonedDateTime(lowerBoundCalendar), getCalendarPrecision(calPrecisionField));
  }

  private static DateRangeBound parseUpperBound(String source) throws ParseException {
    Calendar upperBoundCalendar = prefixTree.parseCalendar(source);
    int calPrecisionField = prefixTree.getCalPrecisionField(upperBoundCalendar);
    if (calPrecisionField < 0) {
      return DateRangeBound.UNBOUNDED;
    }
    ZonedDateTime upperBoundDateTime = toZonedDateTime(upperBoundCalendar);
    DateRangeBound.Precision precision = getCalendarPrecision(calPrecisionField);
    return DateRangeBound.upperBound(upperBoundDateTime, precision);
  }

  private static DateRangeBound.Precision getCalendarPrecision(int calendarPrecision) {
    switch (calendarPrecision) {
      case Calendar.YEAR:
        return DateRangeBound.Precision.YEAR;
      case Calendar.MONTH:
        return DateRangeBound.Precision.MONTH;
      case Calendar.DAY_OF_MONTH:
        return DateRangeBound.Precision.DAY;
      case Calendar.HOUR_OF_DAY:
        return DateRangeBound.Precision.HOUR;
      case Calendar.MINUTE:
        return DateRangeBound.Precision.MINUTE;
      case Calendar.SECOND:
        return DateRangeBound.Precision.SECOND;
      case Calendar.MILLISECOND:
        return DateRangeBound.Precision.MILLISECOND;
      default:
        throw new IllegalStateException("Unsupported date time precision: " + calendarPrecision);
    }
  }

  private static ZonedDateTime toZonedDateTime(Calendar calendar) {
    int year = calendar.get(Calendar.YEAR);
    if (calendar.get(Calendar.ERA) == 0) {
      // BC era; 1 BC == 0 AD, 0 BD == -1 AD, etc
      year -= 1;
      if (year > 0) {
        year = -year;
      }
    }
    LocalDateTime localDateTime =
        LocalDateTime.of(
            year,
            calendar.get(Calendar.MONTH) + 1,
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND));
    localDateTime =
        localDateTime.with(ChronoField.MILLI_OF_SECOND, calendar.get(Calendar.MILLISECOND));
    return ZonedDateTime.of(localDateTime, ZoneOffset.UTC);
  }
}
